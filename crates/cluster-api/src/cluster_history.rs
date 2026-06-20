//! `GET /v1/clusters/range` — выдача исторических кластеров.
//!
//! Терминал тянет историю по (instrument, TF, time range) в columnar
//! JSON — компактный формат (parallel arrays вместо повторяющихся
//! ключей в row-oriented), который сжимается gzip-middleware'ом в
//! 5–10× меньше байт по сравнению с обычным JSON-row-array.
//!
//! Аутентификация — общий bearer middleware (см. `rest.rs`).
//!
//! Жёсткие лимиты:
//! - `MAX_BARS` ограничивает общее число строк (price-buckets) на ответ.
//!   Если запрашиваемый range превысит — отдаём 400, клиент должен бить
//!   запрос на чанки или сужать range.
//! - Запрос идёт на CH без LIMIT — это намеренно: проще получить честный
//!   422 на overshoot, чем тихо обрезать (UI нарисует «дырку» в конце).
//!   Гарантия что мы не выкачаем гигабайт: предварительная оценка
//!   `estimate_max_rows()`.
//!
//! Кэшируемость: ответ для диапазона полностью в прошлом (max_to_ms <
//! now - tf*2) — иммутабельный, ставим `Cache-Control: public, max-age=86400`.
//! Клиент/CDN могут кэшировать долго. Открытый-конец (`to_ms` ≥ now-tf)
//! помечается `no-cache`.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::timeframes::{is_supported_interval, table_name_for};

/// Защита: грубый верхний предел на размер ответа.
///
/// История параметра:
///   v1: 200k — это резало нормальные запросы вроде «5m × 7д» (estimate
///       201_600 → 413). Терминал на estimate'е, не на реальном размере,
///       и его лимит был just-over.
///   v2 (текущее): 500k — даёт реалистичный потолок: 5m × 7д estimate
///       ~200k влезает с запасом; 30s × 24h ~288k влезает; 30m × 30д
///       ~144k влезает. Реальные ответы обычно В 2-3 РАЗА меньше estimate
///       (estimate использует BUCKETS_PER_WINDOW=100 worst-case),
///       так что на сервере мы держим ≤ 500k × 8 fields × 8 bytes ≈
///       30MB в памяти на запрос, gzip срезает до ~3 МБ wire — ОК.
const MAX_BARS: u64 = 500_000;

/// Состояние, нужное хендлеру. Шейрится с `SysMetricsState` намеренно —
/// одно подключение, одни креды на REST. Если в будущем понадобятся
/// разные таймауты — разделим, пока упрощаем.
#[derive(Clone)]
pub struct ClusterHistoryState {
    pub ch_client: reqwest::Client,
    pub ch_url: String,
    pub ch_database: String,
    /// CH user. Дефолт `default` соответствует тому, как пишет
    /// cluster-ingest (без auth). Если деплой делает auth для cluster
    /// user — задаётся в конфиге.
    pub ch_user: String,
    /// Password, пусто если default user без пароля.
    pub ch_password: String,
}

#[derive(Debug, Deserialize)]
pub struct RangeParams {
    pub exchange: String,
    pub market_type: String,
    pub symbol: String,
    pub interval_seconds: u32,
    /// Unix ms (closed-open: [from_ms, to_ms]). `to_ms` исключается.
    pub from_ms: i64,
    pub to_ms: i64,
}

/// Колонки ответа. Все массивы одинаковой длины N = количество
/// (window, price)-пар. Клиент группирует по window_start_ms.
///
/// `price` — int64 в "scaled" единицах (тиков биржи). `bid_qty`/`ask_qty`
/// тоже scaled. Клиент знает scale из `price_scale`/`qty_scale` — мы
/// шлём их в meta верхнего уровня (одно значение на всю выборку:
/// schema гарантирует одинаковый scale в пределах одного symbol).
#[derive(Debug, Serialize)]
pub struct RangeResponse {
    pub interval_seconds: u32,
    pub symbol: String,
    pub exchange: String,
    pub market_type: String,
    /// Шкала цен/количеств. Считываем из первой строки выборки;
    /// если выборка пуста — оба = 0 (клиент должен трактовать пустой
    /// ответ как «данных нет за этот период», не как «scale unknown»).
    pub price_scale: u8,
    pub qty_scale: u8,
    /// Сколько (window, price)-пар. Длина всех columns ниже.
    pub n: usize,
    pub window_start_ms: Vec<i64>,
    pub price: Vec<i64>,
    pub bid_qty: Vec<i64>,
    pub ask_qty: Vec<i64>,
    pub trades: Vec<u32>,
    /// OHLC окна — **per-window**, не per-bucket. Эти 5 массивов длины
    /// `windows_n` (НЕ `n`). Клиент группирует buckets по window_start_ms
    /// и берёт OHLC из i-го элемента по индексу совпадающего window_start
    /// в `windows`. Сохранение — column-major: меньше байт wire'а (5 × 8 ×
    /// windows_n вместо 5 × 8 × n, где обычно n ≈ 30-50 × windows_n).
    pub windows_n: usize,
    pub windows: Vec<i64>,
    pub open: Vec<i64>,
    pub close: Vec<i64>,
    pub high: Vec<i64>,
    pub low: Vec<i64>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

/// Хендлер. Возвращает один из:
/// - 200 + RangeResponse (может быть с n=0)
/// - 400 на bad params (interval не поддержан, market_type не из {spot,perp}, etc.)
/// - 413 если оценка размера превышает MAX_BARS
/// - 502 если CH вернул ошибку
pub async fn cluster_range(
    State(state): State<ClusterHistoryState>,
    Query(q): Query<RangeParams>,
) -> impl IntoResponse {
    // === валидация входа ===
    if let Err(msg) = validate_params(&q) {
        return (StatusCode::BAD_REQUEST, Json(ErrorBody { error: msg })).into_response();
    }

    // Грубая оценка: сколько (window, price) пар максимум поместится в
    // запрошенный диапазон. Реальное число почти всегда сильно меньше,
    // но если оператор спросил `to - from` = 365 дней на 30s — отказ
    // до запроса в CH дешевле, чем после.
    let est_rows = estimate_max_rows(&q);
    if est_rows > MAX_BARS {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorBody {
                error: format!(
                    "estimated rows ({}) exceed cap ({}); narrow the range or use a larger interval_seconds",
                    est_rows, MAX_BARS
                ),
            }),
        )
            .into_response();
    }

    // SQL: прямое чтение для базовых TF (30s/1m), иначе rollup из 1m.
    // FINAL форсирует ReplacingMergeTree дедуп late-arriving дублей —
    // критично корректнее: иначе UI получит две версии одного бара.
    let sql = build_range_sql(&state.ch_database, &q);

    let url = format!("{}/?query={}", state.ch_url.trim_end_matches('/'), urlencode(&sql));
    let mut req = state.ch_client.get(&url);
    if !state.ch_user.is_empty() {
        req = req.basic_auth(&state.ch_user, Some(&state.ch_password));
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "ch http send failed");
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorBody { error: format!("ch unreachable: {e}") }),
            )
                .into_response();
        }
    };
    let status = resp.status();
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorBody { error: format!("ch body read: {e}") }),
            )
                .into_response();
        }
    };
    if !status.is_success() {
        tracing::warn!(%status, ?body, "ch returned non-2xx for cluster range");
        // CH ошибки часто включают конкретный код — пробрасываем как text.
        let truncated: String = body.chars().take(500).collect();
        return (
            StatusCode::BAD_GATEWAY,
            Json(ErrorBody { error: format!("CH {status}: {truncated}") }),
        )
            .into_response();
    }

    // JSONCompactEachRow: одна строка per row, формат
    //   [ts_ms, price, bid_qty, ask_qty, trades, ps, qs, open, close, high, low]
    // Парсим:
    //   - bucket-data: per-row → arrays длины `n`
    //   - per-window OHLC: накопим в SEPARATE arrays длины `windows_n`,
    //     срабатывая на смену ts_ms (входные строки отсортированы по
    //     ts_ms ASC → последовательная группировка без HashMap).
    let mut out = RangeResponse {
        interval_seconds: q.interval_seconds,
        symbol: q.symbol.clone(),
        exchange: q.exchange.clone(),
        market_type: q.market_type.clone(),
        price_scale: 0,
        qty_scale: 0,
        n: 0,
        window_start_ms: Vec::new(),
        price: Vec::new(),
        bid_qty: Vec::new(),
        ask_qty: Vec::new(),
        trades: Vec::new(),
        windows_n: 0,
        windows: Vec::new(),
        open: Vec::new(),
        close: Vec::new(),
        high: Vec::new(),
        low: Vec::new(),
    };
    let est = est_rows.min(MAX_BARS) as usize;
    out.window_start_ms.reserve(est);
    out.price.reserve(est);
    out.bid_qty.reserve(est);
    out.ask_qty.reserve(est);
    out.trades.reserve(est);
    // Каждое окно содержит ~30-50 buckets — `windows_n ≈ n / 40`. Reserve
    // более-менее реалистичную верхнюю границу.
    let est_windows = (est / 20).max(64);
    out.windows.reserve(est_windows);
    out.open.reserve(est_windows);
    out.close.reserve(est_windows);
    out.high.reserve(est_windows);
    out.low.reserve(est_windows);

    let mut last_ts: Option<i64> = None;
    for line in body.lines() {
        if line.is_empty() {
            continue;
        }
        // CH JSONCompactEachRow выдаёт UInt64-числа как строки. Парсим мягко.
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let arr = match v.as_array() {
            Some(a) if a.len() >= 5 => a,
            _ => continue,
        };
        let ts = json_i64(&arr[0]);
        let price = json_i64(&arr[1]);
        let bid = json_i64(&arr[2]);
        let ask = json_i64(&arr[3]);
        let trades = json_u32(&arr[4]);
        let (Some(ts), Some(price), Some(bid), Some(ask), Some(trades)) =
            (ts, price, bid, ask, trades)
        else {
            continue;
        };
        // ps/qs (одинаковы по window-функции для всей выборки) — берём
        // из ПЕРВОЙ валидной строки. Если отсутствуют — оставим 0.
        if out.n == 0 && arr.len() >= 7 {
            if let Some(ps) = json_u32(&arr[5]) {
                out.price_scale = ps.min(u8::MAX as u32) as u8;
            }
            if let Some(qs) = json_u32(&arr[6]) {
                out.qty_scale = qs.min(u8::MAX as u32) as u8;
            }
        }
        // OHLC: фиксируем при смене window_start. arr может иметь 7 элементов
        // (старые legacy записи без OHLC) или 11 (новые). Если 11 — читаем
        // open/close/high/low; если 7 — пишем 0 (UI трактует как "no body").
        if last_ts != Some(ts) {
            last_ts = Some(ts);
            out.windows.push(ts);
            if arr.len() >= 11 {
                out.open.push(json_i64(&arr[7]).unwrap_or(0));
                out.close.push(json_i64(&arr[8]).unwrap_or(0));
                out.high.push(json_i64(&arr[9]).unwrap_or(0));
                out.low.push(json_i64(&arr[10]).unwrap_or(0));
            } else {
                out.open.push(0);
                out.close.push(0);
                out.high.push(0);
                out.low.push(0);
            }
            out.windows_n += 1;
        }

        out.window_start_ms.push(ts);
        out.price.push(price);
        out.bid_qty.push(bid);
        out.ask_qty.push(ask);
        out.trades.push(trades);
        out.n += 1;

        // На случай если CH вернул больше чем мы оценили (например при
        // плотном символе) — обрезаем по жёсткому потолку, чтобы
        // не насадить терминал OOM'ом.
        if out.n as u64 >= MAX_BARS {
            tracing::warn!(
                symbol = %q.symbol,
                interval_seconds = q.interval_seconds,
                "hit MAX_BARS during CH parse; truncating"
            );
            break;
        }
    }

    // Cache-Control:
    // - Диапазон строго в прошлом (to_ms < now - 2*tf) → immutable, час.
    // - Иначе короткий no-store (текущая свеча будет меняться от live).
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let tf_ms = q.interval_seconds as i64 * 1000;
    let cache_value = if q.to_ms + 2 * tf_ms < now_ms {
        "public, max-age=86400, immutable"
    } else {
        "no-store"
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, cache_value.parse().unwrap());

    (StatusCode::OK, headers, Json(out)).into_response()
}

/// Build the CH query for a range request.
///
/// Только базовые TF (30s, 1m) живут в CH как отдельные таблицы — их пишет
/// ingest. Остальные (5m/15m/30m/1h/4h/1d) НЕ агрегируются в RAM, а
/// **досчитываются из `clusters_1m` на чтении** (rollup): footprint аддитивен
/// по времени, поэтому bucket за длинное окно = сумма bucket'ов 1m-под-окон с
/// тем же price. Границы окна (`toStartOfInterval(.., N SECOND, 'UTC')`)
/// совпадают byte-в-byte с тем, что выдал бы живой аггрегатор
/// (`window_start = t - t % (N*1000)` от epoch UTC), так что прямые и
/// досчитанные окна консистентны.
///
/// Обе ветки возвращают 11 колонок в одном порядке —
/// `[ts_ms, price, bid_qty, ask_qty, trades, price_scale, qty_scale, open, close, high, low]` —
/// так что парсер ответа общий.
fn build_range_sql(db: &str, q: &RangeParams) -> String {
    let ex = sql_escape(&q.exchange);
    let mt = sql_escape(&q.market_type);
    let sym = sql_escape(&q.symbol);
    let filter = format!(
        "exchange = '{ex}' AND market_type = '{mt}' AND symbol = '{sym}' \
         AND window_start >= fromUnixTimestamp64Milli({from}) \
         AND window_start <  fromUnixTimestamp64Milli({to})",
        from = q.from_ms,
        to = q.to_ms,
    );

    if matches!(q.interval_seconds, 30 | 60) {
        // Прямое чтение базовой таблицы. OHLC — детерминированной агрегацией per window,
        // JOIN'нутой к per-bucket строкам. Раньше было `any(open) OVER (PARTITION BY window_start)`:
        // OHLC НЕ в ORDER BY (sort key), поэтому разные bucket-строки одного окна несут РАЗНЫЙ OHLC
        // (записаны в разные моменты), а `any()` берёт ПРОИЗВОЛЬНУЮ строку → нестабильный/неверный
        // open/close на текущей свече + legacy DEFAULT-0 строки (migration 004) травили low/open.
        // Фикс: open = maxIf(open,open>0) (open иммутабелен → все ненулевые равны, max убирает 0);
        // close = argMaxIf(close, ingested_at, close>0) (последний по времени записи ненулевой close);
        // high/low = maxIf/minIf с >0-гардом. -If только в GROUP BY (переносимо, без оконных -If).
        let table = table_name_for(q.interval_seconds);
        return format!(
            "SELECT \
               toUnixTimestamp64Milli(t.window_start) AS ts_ms, \
               t.price, t.bid_qty, t.ask_qty, t.trades, \
               w.ps, w.qs, w.o, w.c, w.h, w.l \
             FROM {db}.{table} AS t FINAL \
             INNER JOIN ( \
               SELECT window_start, \
                 maxIf(price_scale, price_scale > 0) AS ps, \
                 maxIf(qty_scale, qty_scale > 0)     AS qs, \
                 maxIf(open, open > 0)               AS o, \
                 argMaxIf(close, ingested_at, close > 0) AS c, \
                 maxIf(high, high > 0)               AS h, \
                 minIf(low, low > 0)                 AS l \
               FROM {db}.{table} FINAL \
               WHERE {filter} \
               GROUP BY window_start \
             ) AS w ON t.window_start = w.window_start \
             WHERE {filter} \
             ORDER BY t.window_start, t.price \
             FORMAT JSONCompactEachRow",
        );
    }

    // Rollup из 1m. Две агрегации над одним отфильтрованным срезом:
    //   b — per (rolled_window, price): сумма bid/ask/trades (+ scale).
    //   w — per rolled_window: OHLC (open=первое под-окно, close=последнее,
    //       high=max, low=min) через argMin/argMax (plain-агрегаты — без
    //       оконных функций, чтобы не зависеть от их поддержки).
    let n = q.interval_seconds;
    let base = table_name_for(60); // clusters_1m — общий делитель всех ≥1m TF
    // NB: toStartOfInterval(.., SECOND) → DateTime (not DateTime64), so
    // toUnixTimestamp64Milli rejects it. Windows are second-aligned →
    // seconds*1000 = ms exactly.
    //
    // OHLC rollup в ДВА уровня (раньше argMin/argMax/max/min прямо по сырым строкам — брало OHLC из
    // ПРОИЗВОЛЬНОЙ bucket-строки и тонуло на legacy DEFAULT-0):
    //   s — collapse КАЖДОГО 1m под-окна к его OHLC (по всем его price-строкам): open=maxIf(>0)
    //       (иммутабелен), close=argMaxIf по ingested_at (истинный последний), high/low=maxIf/minIf(>0).
    //   w — rollup per rolled-window: open = argMinIf(sub_open, sub_ws, >0) (open ПЕРВОГО под-окна),
    //       close = argMaxIf(sub_close, sub_ws, >0) (close ПОСЛЕДНЕГО под-окна), high/low = max/min.
    // Всё в GROUP BY (переносимо). >0-гарды убирают legacy-0. Это и есть детерминированный,
    // консистентный open/close для досчитанных TF (5m/15m/…).
    format!(
        "SELECT \
           toUnixTimestamp(b.rw) * 1000 AS ts_ms, \
           b.price, b.bid_qty, b.ask_qty, b.trades, b.ps, b.qs, \
           w.o, w.c, w.h, w.l \
         FROM ( \
           SELECT \
             toStartOfInterval(window_start, INTERVAL {n} SECOND, 'UTC') AS rw, \
             price, \
             sum(bid_qty) AS bid_qty, sum(ask_qty) AS ask_qty, sum(trades) AS trades, \
             maxIf(price_scale, price_scale > 0) AS ps, maxIf(qty_scale, qty_scale > 0) AS qs \
           FROM {db}.{base} FINAL \
           WHERE {filter} \
           GROUP BY rw, price \
         ) AS b \
         INNER JOIN ( \
           SELECT rw, \
             argMinIf(sub_open,  sub_ws, sub_open  > 0) AS o, \
             argMaxIf(sub_close, sub_ws, sub_close > 0) AS c, \
             maxIf(sub_high, sub_high > 0) AS h, \
             minIf(sub_low,  sub_low  > 0) AS l \
           FROM ( \
             SELECT \
               toStartOfInterval(window_start, INTERVAL {n} SECOND, 'UTC') AS rw, \
               window_start AS sub_ws, \
               maxIf(open, open > 0)                    AS sub_open, \
               argMaxIf(close, ingested_at, close > 0) AS sub_close, \
               maxIf(high, high > 0)                    AS sub_high, \
               minIf(low, low > 0)                      AS sub_low \
             FROM {db}.{base} FINAL \
             WHERE {filter} \
             GROUP BY rw, window_start \
           ) \
           GROUP BY rw \
         ) AS w ON b.rw = w.rw \
         ORDER BY ts_ms, b.price \
         FORMAT JSONCompactEachRow",
    )
}

fn validate_params(q: &RangeParams) -> Result<(), String> {
    if q.symbol.is_empty() || q.symbol.len() > 32 {
        return Err("symbol: required, max 32 chars".into());
    }
    if !q
        .symbol
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err("symbol: ASCII alphanumeric + _- only".into());
    }
    if !matches!(q.market_type.as_str(), "spot" | "perp") {
        return Err("market_type: 'spot' | 'perp'".into());
    }
    if q.exchange.is_empty() || q.exchange.len() > 32 {
        return Err("exchange: required, max 32 chars".into());
    }
    if !q.exchange.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err("exchange: ASCII alphanumeric only (e.g. BINANCEF)".into());
    }
    if !is_supported_interval(q.interval_seconds) {
        return Err(format!(
            "interval_seconds: {} not supported; use one of 30/60/300/900/3600/14400/86400",
            q.interval_seconds
        ));
    }
    if q.from_ms < 0 || q.to_ms < 0 {
        return Err("from_ms/to_ms: must be >= 0".into());
    }
    if q.to_ms <= q.from_ms {
        return Err("to_ms must be > from_ms".into());
    }
    Ok(())
}

/// Грубая оценка: при идеальной плотности (каждое окно × некоторое
/// число price-bucket'ов) сколько строк теоретически можно получить.
/// Цель — отсечь явно нереалистичные запросы (год 30s по одному symbol).
///
/// Предположение: 50 buckets/window средне-волатильного символа.
/// На очень волатильных BTC может быть 200+, но реальный count'а
/// проверка происходит уже после CH-фетча (если CH вернул больше —
/// мы обрезаем по MAX_BARS, см. parse-loop с break при out.n >= MAX_BARS).
fn estimate_max_rows(q: &RangeParams) -> u64 {
    const BUCKETS_PER_WINDOW: u64 = 50;
    let span_ms = (q.to_ms - q.from_ms).max(0) as u64;
    let tf_ms = q.interval_seconds as u64 * 1000;
    let windows = span_ms / tf_ms.max(1);
    windows.saturating_mul(BUCKETS_PER_WINDOW)
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
        }
    }
    out
}

fn json_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn json_u32(v: &serde_json::Value) -> Option<u32> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().and_then(|x| u32::try_from(x).ok()),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(interval: u32, from: i64, to: i64) -> RangeParams {
        RangeParams {
            exchange: "BINANCEF".into(),
            market_type: "perp".into(),
            symbol: "BTCUSDT".into(),
            interval_seconds: interval,
            from_ms: from,
            to_ms: to,
        }
    }

    #[test]
    fn validate_happy_path() {
        validate_params(&p(60, 0, 60_000)).unwrap();
    }

    #[test]
    fn validate_rejects_unsupported_interval() {
        assert!(validate_params(&p(45, 0, 60_000)).is_err());
    }

    #[test]
    fn validate_rejects_bad_market() {
        let mut q = p(60, 0, 60_000);
        q.market_type = "futures".into();
        assert!(validate_params(&q).is_err());
    }

    #[test]
    fn validate_rejects_symbol_with_sql_chars() {
        let mut q = p(60, 0, 60_000);
        q.symbol = "BTC'OR'1=1".into();
        assert!(validate_params(&q).is_err());
    }

    #[test]
    fn validate_rejects_inverted_range() {
        assert!(validate_params(&p(60, 1000, 500)).is_err());
        assert!(validate_params(&p(60, 1000, 1000)).is_err());
    }

    #[test]
    fn estimate_caps_silly_ranges() {
        // 30s × 365 дней × 50 buckets/win ≈ 52M строк → отсекаем.
        let yr = p(30, 0, 365 * 86_400_000);
        assert!(estimate_max_rows(&yr) > MAX_BARS);
        // 1m × 1 час = 60 окон × 50 = 3000 → пропускаем.
        let hr = p(60, 0, 3_600_000);
        assert!(estimate_max_rows(&hr) < MAX_BARS);
        // Регресс: ранее 5m × 7d давал estimate 201_600 vs cap 200_000
        // (фейлило с 413). Теперь cap=500k, estimate=50/win →
        // 7d/5min × 50 = 100_800, спокойно влезает.
        let five_min_week = p(300, 0, 7 * 86_400_000);
        assert!(estimate_max_rows(&five_min_week) < MAX_BARS,
            "5m × 7d must fit: estimate={}", estimate_max_rows(&five_min_week));
    }

    #[test]
    fn json_i64_handles_string_and_number() {
        assert_eq!(json_i64(&serde_json::json!(42)), Some(42));
        assert_eq!(json_i64(&serde_json::json!("42")), Some(42));
        assert_eq!(json_i64(&serde_json::json!(-1)), Some(-1));
        assert_eq!(json_i64(&serde_json::json!(null)), None);
    }

    #[test]
    fn sql_escape_quotes_a_single_quote() {
        assert_eq!(sql_escape("hello"), "hello");
        assert_eq!(sql_escape("o'brien"), "o''brien");
    }

    #[test]
    fn base_tfs_read_their_table_directly() {
        let s30 = build_range_sql("clusters", &p(30, 0, 60_000));
        assert!(s30.contains("clusters.clusters_30s FINAL"));
        assert!(!s30.contains("toStartOfInterval"), "30s must not roll up");
        let s60 = build_range_sql("clusters", &p(60, 0, 60_000));
        assert!(s60.contains("clusters.clusters_1m FINAL"));
        assert!(!s60.contains("toStartOfInterval"), "1m must not roll up");
    }

    #[test]
    fn long_tfs_roll_up_from_1m() {
        for iv in [300u32, 900, 1800, 3600, 14400, 86400] {
            let s = build_range_sql("clusters", &p(iv, 0, 86_400_000));
            assert!(s.contains("clusters.clusters_1m FINAL"), "{iv}: base must be 1m");
            assert!(!s.contains("clusters_5m") && !s.contains("clusters_1d"),
                "{iv}: must not touch the stale per-TF tables");
            assert!(s.contains(&format!("INTERVAL {iv} SECOND")), "{iv}: rolled boundary");
            assert!(
                s.contains("sum(bid_qty)")
                    && s.contains("argMinIf(sub_open")
                    && s.contains("argMaxIf(sub_close"),
                "{iv}: additive buckets + deterministic two-level OHLC rollup",
            );
            // 11-column output order preserved for the shared parser.
            assert!(s.contains("b.price, b.bid_qty, b.ask_qty, b.trades, b.ps, b.qs, w.o, w.c, w.h, w.l"));
        }
    }
}
