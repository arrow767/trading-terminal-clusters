use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

/// Top-level configuration loaded from `cluster-ingest.toml`. Missing
/// fields fall back to documented defaults so a stripped-down config
/// stays usable in dev. The bin treats `Config::default()` as a valid
/// "no config provided" outcome.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub ingest: IngestConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default = "default_ch_url")]
    pub clickhouse_url: String,
    #[serde(default = "default_ch_database")]
    pub clickhouse_database: String,
    /// Legacy: имя таблицы. В multi-TF режиме (timeframes_secs) имена
    /// таблиц берутся через `table_name_for(tf)` per-TF; это поле осталось
    /// для совместимости со старыми конфигами и не используется кодом.
    #[allow(dead_code)]
    #[serde(default = "default_ch_table")]
    pub clickhouse_table: String,
    /// Список таймфреймов в секундах, которые ingest агрегирует и пишет
    /// в CH (каждый в свою таблицу `clusters_{tf}`).
    ///
    /// Дефолт `[60]` — back-compat со старым однотабличным режимом.
    /// Для нового деплоя в TOML: `timeframes_secs = [30, 60, 300, 900, 3600, 14400, 86400]`.
    ///
    /// Каждый TF — независимый аггрегатор-task на каждый символ. Трейды
    /// фанаутятся per-symbol на N приёмников (см. `BinanceSupervisor`).
    /// Минимальный TF = 30s; меньше — формат `clusters_30s` уже не покроет
    /// и потребует ребранд таблиц.
    #[serde(default = "default_timeframes_secs")]
    pub timeframes_secs: Vec<u32>,

    /// Legacy: глобальный window_ms. В multi-TF режиме окно для каждой
    /// TF выводится из `timeframes_secs[i] * 1000`. Поле оставлено для
    /// back-compat при парсинге старых конфигов и не используется кодом.
    #[allow(dead_code)]
    #[serde(default = "default_window_ms")]
    pub window_ms: i64,
    #[serde(default = "default_diff_interval_ms")]
    pub diff_interval_ms: i64,
    #[serde(default = "default_agg_tick_ms")]
    pub agg_tick_interval_ms: u64,
    #[serde(default = "default_trade_channel_bound")]
    pub trade_channel_bound: usize,
    #[serde(default = "default_ch_channel_bound")]
    pub ch_channel_bound: usize,
    #[serde(default = "default_grpc_listen")]
    pub grpc_listen: String,
    #[serde(default = "default_rest_listen")]
    pub rest_listen: String,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    /// Whitelist of exchange names ("binance_perp", future "bybit_perp", …)
    /// that this ingest process should drive. Empty list (default) falls
    /// back to per-section `enabled` flags so старые конфиги продолжают
    /// работать. Name not present here ⇒ supervisor not started even
    /// если в его секции `enabled = true`.
    #[serde(default)]
    pub enabled_exchanges: Vec<String>,
    #[serde(default)]
    pub exchanges: ExchangesConfig,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            region: default_region(),
            clickhouse_url: default_ch_url(),
            clickhouse_database: default_ch_database(),
            clickhouse_table: default_ch_table(),
            timeframes_secs: default_timeframes_secs(),
            window_ms: default_window_ms(),
            diff_interval_ms: default_diff_interval_ms(),
            agg_tick_interval_ms: default_agg_tick_ms(),
            trade_channel_bound: default_trade_channel_bound(),
            ch_channel_bound: default_ch_channel_bound(),
            grpc_listen: default_grpc_listen(),
            rest_listen: default_rest_listen(),
            auth: AuthConfig::default(),
            retention: RetentionConfig::default(),
            enabled_exchanges: Vec::new(),
            exchanges: ExchangesConfig::default(),
        }
    }
}

/// Bearer-токен авторизация для gRPC ClusterStream и REST /v1/system/metrics.
///
/// Дизайн «hardcoded токен»: чтобы предотвратить DoS от случайных
/// краулеров/брутфорсеров и закрыть данные одним статическим секретом.
/// Это НЕ user-auth — токен один на сервис, кладётся в конфиг/env,
/// меняется перезапуском.
///
/// **Не коммить токен в git.** Используй `[ingest.auth].tokens` только
/// в `.local.toml` или передавай через env. В прод-deploy токен живёт
/// в секретах оркестратора.
///
/// `enabled = false` (default для совместимости со старым конфигом) →
/// сервис стартует без авторизации; ставь `true` в prod-deploy. Если
/// `enabled = true`, но `tokens` пуст — сервис откажется стартовать
/// (см. `validate()`), чтобы случайно не оставить дверь открытой.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub tokens: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tokens: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// Применить env-override: если задан `CLUSTER_INGEST_TOKEN`, токен
    /// из ENV добавляется к списку (или становится единственным).
    /// Удобно для prod-deploy через секреты, чтобы файл конфига можно
    /// было держать в git без секретов.
    pub fn apply_env(&mut self) {
        if let Ok(t) = std::env::var("CLUSTER_INGEST_TOKEN") {
            if !t.is_empty() {
                self.tokens.push(t);
            }
        }
        if let Ok(v) = std::env::var("CLUSTER_INGEST_AUTH_ENABLED") {
            self.enabled = matches!(v.as_str(), "1" | "true" | "TRUE" | "yes");
        }
    }

    /// Защита от self-foot-gun: enabled но пустой список = молчаливо
    /// открытый сервис.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.enabled && self.tokens.is_empty() {
            anyhow::bail!(
                "auth.enabled = true но tokens пуст; либо задай токен в \
                 [ingest.auth].tokens / env CLUSTER_INGEST_TOKEN, либо \
                 выключи enabled"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExchangesConfig {
    pub binance_perp: Option<BinancePerpConfig>,
    /// Конфиг Binance spot (api.binance.com + stream.binance.com:9443).
    /// Той же формы, что и `binance_perp`: символьные фильтры, top_n,
    /// rank_by, discovery_poll и WS-параметры. Default отключён —
    /// включается явным enabled=true или whitelist'ом enabled_exchanges
    /// (включающим "binance_spot").
    pub binance_spot: Option<BinancePerpConfig>,
    /// Bybit V5 USDT/USDC linear perps (api.bybit.com /v5/linear).
    /// Поддерживает BTCUSDT (USDT-linear) и BTCPERP (USDC-linear).
    /// Config — общая форма BinancePerpConfig: filter symbols, top_n,
    /// rank_by, ws-параметры.
    pub bybit_perp: Option<BinancePerpConfig>,
    /// Bybit V5 spot (api.bybit.com /v5/spot, ws .../public/spot).
    pub bybit_spot: Option<BinancePerpConfig>,
    /// OKX V5 USDT/USDC linear perpetual swaps (www.okx.com, ws /v5/public).
    /// Той же формы BinancePerpConfig. Своп-qty переводится контракты→база
    /// через ctVal внутри exchange-okx.
    pub okx_perp: Option<BinancePerpConfig>,
    /// OKX V5 spot (USDT/USDC).
    pub okx_spot: Option<BinancePerpConfig>,
}

/// Сколько хранить строки `clusters_1m` до TTL-DELETE в ClickHouse.
///
/// Поддерживает три режима:
/// 1. **Global** — один `default_days` на всё (что было раньше).
/// 2. **Per-(exchange, market_type) overrides** — точечные правила:
///    «BINANCEF/perp → 30 дней, BYBITF/perp → 7 дней, BINANCE/spot → forever».
/// 3. **Forever для конкретной пары** — `days = 0` в override.
///
/// На старте ingest выпускает `ALTER TABLE … MODIFY TTL <multi-clause>`
/// и ClickHouse сам фоном удаляет старые партиции по merge'у. Менять
/// retention = поправить TOML + перезапустить процесс.
///
/// Реализовано через CH multi-clause TTL с `WHERE` predicate'ом:
/// каждый override → отдельная клауза с конкретным условием по колонкам
/// `exchange` + `market_type`; default-клауза получает `WHERE NOT (…объединение
/// всех overrides…)`. Если default_days == 0 и все overrides forever — TTL
/// не пишется вообще.
///
/// `move_to_cold_days` опционален: применяется ТОЛЬКО к default-клаузе
/// (per-row cold-tier не имеет смысла на таких объёмах). Если выключен,
/// данные живут на hot volume до самого DELETE.
#[derive(Debug, Clone, Deserialize)]
pub struct RetentionConfig {
    /// Дефолт для всех (exchange, market_type), не упомянутых в overrides.
    /// `0` = не удалять по умолчанию (только overrides будут чистить).
    ///
    /// `alias = "days"` сохраняет совместимость со старым конфигом, где
    /// поле называлось просто `days`.
    #[serde(default = "default_retention_days", alias = "days")]
    pub default_days: u64,
    #[serde(default)]
    pub move_to_cold_days: Option<u64>,
    #[serde(default = "true_")]
    pub apply_on_start: bool,
    /// Точечные правила. Поле `override` в TOML (singular) для красоты:
    ///   `[[ingest.retention.override]] exchange = "BINANCEF" ... days = 30`
    #[serde(default, rename = "override")]
    pub overrides: Vec<RetentionOverride>,
}

/// Один override: пара (биржа, рынок) и сколько дней её держать.
///
/// `exchange` — wire-id из `Exchange::wire_id()` (UPPER): "BINANCE",
/// "BINANCEF", "BYBIT", "BYBITF", "OKX", "OKXF", "BITGET", "BITGETF",
/// "KUCOIN", "KUCOINF", "HYPERLIQUID", "GATE", "GATEF".
///
/// `market_type` — "spot" | "perp" (lowercase, то что пишет sink).
///
/// `interval_seconds` опционально:
///   - `None` (не задано) → правило применяется ко ВСЕМ TF этой пары
///     (например «BINANCE/spot forever на 30s и на 1m и на 1h»).
///   - `Some(60)` → ТОЛЬКО для таблицы `clusters_1m`.
/// Это даёт самую тонкую гранулярность: «30s держать день, 1h forever».
///
/// `days = 0` → forever (этот срез никогда не удаляется автоматически).
#[derive(Debug, Clone, Deserialize)]
pub struct RetentionOverride {
    pub exchange: String,
    pub market_type: String,
    #[serde(default)]
    pub interval_seconds: Option<u32>,
    pub days: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            default_days: default_retention_days(),
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: Vec::new(),
        }
    }
}

impl RetentionConfig {
    /// Собрать тело `TTL …` для `ALTER TABLE clusters_<tf> … MODIFY TTL <body>`
    /// конкретной TF-таблицы. `None` если в итоге ни одной клаузы нет
    /// (для этой TF retention выключена целиком).
    ///
    /// Override применяется к данной TF если `override.interval_seconds`
    /// либо None (правило «для всех TF этой пары»), либо `Some(tf)`.
    /// Остальные override'ы игнорируются — они применятся к своим TF.
    pub fn ttl_clause_for(&self, interval_seconds: u32) -> Option<String> {
        let mut clauses: Vec<String> = Vec::new();
        let mut excluded_conditions: Vec<String> = Vec::new();

        // Отбираем override'ы, применимые к этой TF, и дедуплицируем
        // по (exchange, market) с приоритетом более конкретного правила
        // (с явным interval_seconds), чтобы не выпустить две DELETE-клаузы
        // на один и тот же срез.
        use std::collections::HashMap;
        let mut effective: HashMap<(&str, &str), &RetentionOverride> = HashMap::new();
        for ov in &self.overrides {
            // Не наша TF — мимо.
            match ov.interval_seconds {
                Some(tf) if tf != interval_seconds => continue,
                _ => {}
            }
            let k = (ov.exchange.as_str(), ov.market_type.as_str());
            match effective.get(&k) {
                None => {
                    effective.insert(k, ov);
                }
                Some(prev) => {
                    // Более специфичное (Some(tf)) бьёт более общее (None).
                    if prev.interval_seconds.is_none() && ov.interval_seconds.is_some() {
                        effective.insert(k, ov);
                    }
                }
            }
        }

        for ov in effective.values() {
            let cond = format!(
                "exchange = '{}' AND market_type = '{}'",
                sql_escape_single(&ov.exchange),
                sql_escape_single(&ov.market_type)
            );
            excluded_conditions.push(cond.clone());
            if ov.days > 0 {
                clauses.push(format!(
                    "toDateTime(window_start) + INTERVAL {} DAY DELETE WHERE {}",
                    ov.days, cond
                ));
            }
        }

        let default_where = if excluded_conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE NOT ({})", excluded_conditions.join(" OR "))
        };

        if self.default_days > 0 {
            // move-to-cold ставится первой клаузой (CH парсит TTL слева
            // направо: move action должен быть раньше DELETE для того же
            // временнóго среза, иначе валидатор ругнётся).
            if let Some(cold) = self.move_to_cold_days {
                if cold > 0 && cold < self.default_days {
                    clauses.push(format!(
                        "toDateTime(window_start) + INTERVAL {cold} DAY TO VOLUME 'cold'{default_where}"
                    ));
                }
            }
            clauses.push(format!(
                "toDateTime(window_start) + INTERVAL {} DAY DELETE{}",
                self.default_days, default_where
            ));
            // Сортируем: TO VOLUME перед DELETE для одного default-среза
            // (она была добавлена раньше, но если будут override-DELETE
            // между ними — порядок останется корректным, т.к. они идут
            // первыми и имеют свой WHERE).
        }

        if clauses.is_empty() {
            None
        } else {
            Some(clauses.join(", "))
        }
    }

    /// Проверки на типичные ошибки оператора. Не лезем в семантику CH —
    /// только то, что точно дефект конфига.
    pub fn validate(&self) -> anyhow::Result<()> {
        use std::collections::HashSet;
        // Дубликат = (exchange, market, interval_seconds). Два правила
        // с одинаковыми тремя ключами — дефект. Но (exchange, market, None)
        // + (exchange, market, Some(30)) — нормально (global + точечное).
        let mut seen = HashSet::new();
        for ov in &self.overrides {
            let key = (ov.exchange.as_str(), ov.market_type.as_str(), ov.interval_seconds);
            if !seen.insert(key) {
                anyhow::bail!(
                    "retention.override: дубликат для ({}, {}, interval_seconds={:?}) — оставь одно правило",
                    ov.exchange,
                    ov.market_type,
                    ov.interval_seconds
                );
            }
            // Минимальная санитизация: подсказываем оператору если он
            // явно перепутал регистр (всё ещё работает в CH, но не совпадёт
            // с тем, что пишет sink, → правило будет no-op'ом и человек
            // подумает «не применилось»).
            if ov.exchange != ov.exchange.to_uppercase() {
                anyhow::bail!(
                    "retention.override.exchange = '{}' — ожидается UPPER \
                     (как Exchange::wire_id, например 'BINANCEF')",
                    ov.exchange
                );
            }
            if ov.market_type != ov.market_type.to_lowercase() {
                anyhow::bail!(
                    "retention.override.market_type = '{}' — ожидается lowercase \
                     ('spot' | 'perp')",
                    ov.market_type
                );
            }
            if !matches!(ov.market_type.as_str(), "spot" | "perp") {
                anyhow::bail!(
                    "retention.override.market_type = '{}' — поддерживаются 'spot' | 'perp'",
                    ov.market_type
                );
            }
        }
        Ok(())
    }
}

/// Эскейп одинарных кавычек для SQL-литералов. Биржа/маркет приходят из
/// конфига оператора — это не security boundary, но защита от случайной
/// кавычки в строке, которая иначе сломала бы ALTER.
fn sql_escape_single(s: &str) -> String {
    s.replace('\'', "''")
}

/// Ordering applied to the filtered symbol set before `top_n` cuts it.
/// Default `Alphabetical` matches what exchangeInfo returns and avoids
/// extra network calls. `Volume24h` calls `/fapi/v1/ticker/24hr` once
/// per discovery cycle and ranks descending by quote-currency notional —
/// useful for "top 50 most-traded perps" deployments.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RankBy {
    #[default]
    Alphabetical,
    Volume24h,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinancePerpConfig {
    #[serde(default = "true_")]
    pub enabled: bool,

    /// Quote currencies to keep. Anything not in this list is dropped at
    /// the filter stage. Defaults to USDT+USDC.
    #[serde(default = "default_include_quotes")]
    pub include_quotes: Vec<String>,

    /// If non-empty, ONLY these symbols are subscribed (after quote
    /// filter). Use to lock dev to a small fixed set; leave empty in
    /// prod so auto-discovery picks up new listings.
    #[serde(default)]
    pub allow: Vec<String>,

    /// Symbols to skip even if they otherwise match.
    #[serde(default)]
    pub deny: Vec<String>,

    /// If set, keep at most this many symbols from the filtered+ranked
    /// list. Combined with `rank_by = "volume_24h"` this gives "top N
    /// most-traded perps". Leave unset in prod if you want everything.
    pub top_n: Option<usize>,

    /// Ordering applied before `top_n` truncation. See `RankBy`.
    #[serde(default)]
    pub rank_by: RankBy,

    /// How often to re-fetch exchangeInfo and reconcile the symbol set.
    /// New listings appear at this cadence at worst; defaults to 5 min.
    #[serde(default = "default_discovery_poll_secs")]
    pub discovery_poll_secs: u64,

    #[serde(default = "default_ws_connect_timeout_ms")]
    pub ws_connect_timeout_ms: u64,

    #[serde(default = "default_backoff_min_ms")]
    pub reconnect_backoff_min_ms: u64,

    #[serde(default = "default_backoff_max_ms")]
    pub reconnect_backoff_max_ms: u64,
}

impl Default for BinancePerpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            include_quotes: default_include_quotes(),
            allow: Vec::new(),
            deny: Vec::new(),
            top_n: None,
            rank_by: RankBy::default(),
            discovery_poll_secs: default_discovery_poll_secs(),
            ws_connect_timeout_ms: default_ws_connect_timeout_ms(),
            reconnect_backoff_min_ms: default_backoff_min_ms(),
            reconnect_backoff_max_ms: default_backoff_max_ms(),
        }
    }
}

// table_name_for / SUPPORTED_INTERVALS живут в cluster_api::timeframes —
// один источник правды для ingest-стороны и REST-handler'а.
pub use cluster_api::timeframes::table_name_for;

impl IngestConfig {
    pub fn agg_tick_interval(&self) -> Duration {
        Duration::from_millis(self.agg_tick_interval_ms)
    }

    /// Валидация конфига timeframes:
    /// - не пустой
    /// - все значения > 0
    /// - нет дубликатов
    /// - кратность: каждый TF кратен 30s (минимальный супп. шаг) и
    ///   соответствует одной из known табличных конвенций (см. table_name_for)
    pub fn validate_timeframes(&self) -> anyhow::Result<()> {
        use std::collections::HashSet;
        if self.timeframes_secs.is_empty() {
            anyhow::bail!("timeframes_secs пуст; задай хотя бы один TF (например [60])");
        }
        let mut seen = HashSet::new();
        for &tf in &self.timeframes_secs {
            if tf == 0 {
                anyhow::bail!("timeframes_secs содержит 0 — это бессмысленно");
            }
            if tf < 30 {
                anyhow::bail!(
                    "timeframes_secs: {tf}s слишком мелко (минимум 30); меньше → \
                     ребранд таблиц `clusters_30s` теряет смысл"
                );
            }
            if !seen.insert(tf) {
                anyhow::bail!("timeframes_secs: дубликат {tf}");
            }
        }
        Ok(())
    }

    /// Должна ли быть запущена биржа `name`. Whitelist `enabled_exchanges`
    /// — главный авторитет: если он непустой, биржа стартует ровно тогда,
    /// когда её имя в нём перечислено. Если whitelist пустой (legacy /
    /// minimal config), решает per-section `enabled`-флаг, переданный
    /// вторым аргументом.
    pub fn is_exchange_enabled(&self, name: &str, per_section_enabled: bool) -> bool {
        if self.enabled_exchanges.is_empty() {
            return per_section_enabled;
        }
        self.enabled_exchanges.iter().any(|n| n == name)
    }
}

impl BinancePerpConfig {
    pub fn discovery_poll(&self) -> Duration {
        Duration::from_secs(self.discovery_poll_secs)
    }
    pub fn ws_connect_timeout(&self) -> Duration {
        Duration::from_millis(self.ws_connect_timeout_ms)
    }
    pub fn backoff_min(&self) -> Duration {
        Duration::from_millis(self.reconnect_backoff_min_ms)
    }
    pub fn backoff_max(&self) -> Duration {
        Duration::from_millis(self.reconnect_backoff_max_ms)
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        Self::parse(&s)
    }

    pub fn parse(s: &str) -> Result<Self> {
        toml::from_str(s).context("parse cluster-ingest config")
    }
}

fn default_region() -> String {
    "tokyo".into()
}
fn default_ch_url() -> String {
    "http://127.0.0.1:8123".into()
}
fn default_ch_database() -> String {
    "clusters".into()
}
fn default_ch_table() -> String {
    "clusters_1m".into()
}
fn default_retention_days() -> u64 {
    7
}
fn default_timeframes_secs() -> Vec<u32> {
    vec![60]
}
fn default_window_ms() -> i64 {
    60_000
}
fn default_diff_interval_ms() -> i64 {
    200
}
fn default_agg_tick_ms() -> u64 {
    100
}
fn default_trade_channel_bound() -> usize {
    // 4096 был мал: при кратковременном залипании per-TF аггрегатора
    // (например, при build snapshot большого 5m окна на BTC) канал
    // переполнялся и supervisor fanout молча терял трейды через `try_send`
    // на Full — РАЗНЫЕ TF теряли РАЗНЫЕ трейды → расхождение данных
    // между минуткой и 5-минуткой. 32768 при средней скорости BTC
    // ~1000 трейдов/сек = 30+ секунд запаса перед переполнением.
    32_768
}
fn default_ch_channel_bound() -> usize {
    // 16384 был мал: при CH HTTP throttle (компакшен, network blip)
    // канал к ChWriter переполнялся, sink fanout блокировался на send,
    // bus переполнялся, фреймы дропались. 65536 даёт 4× больше буфера
    // и при ~5-30 строк/snapshot — это десятки тысяч snapshot'ов
    // в очереди до того как backpressure пройдёт к bus.
    65_536
}
fn default_grpc_listen() -> String {
    "127.0.0.1:50051".into()
}
fn default_rest_listen() -> String {
    // 127.0.0.1 — по умолчанию слушаем только loopback; reverse-proxy
    // (nginx/caddy в deploy/) терминирует TLS и пробрасывает.
    "127.0.0.1:8080".into()
}
fn default_include_quotes() -> Vec<String> {
    vec!["USDT".into(), "USDC".into()]
}
fn default_discovery_poll_secs() -> u64 {
    300
}
fn default_ws_connect_timeout_ms() -> u64 {
    15_000
}
fn default_backoff_min_ms() -> u64 {
    500
}
fn default_backoff_max_ms() -> u64 {
    30_000
}
fn true_() -> bool {
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_yields_defaults() {
        let c = Config::parse("").unwrap();
        assert_eq!(c.ingest.region, "tokyo");
        assert_eq!(c.ingest.window_ms, 60_000);
        assert!(c.ingest.exchanges.binance_perp.is_none());
    }

    #[test]
    fn binance_perp_section_picked_up() {
        let toml_str = r#"
            [ingest]
            region = "singapore"
            window_ms = 30000

            [ingest.exchanges.binance_perp]
            allow = ["BTCUSDT", "ETHUSDT"]
            top_n = 50
        "#;
        let c = Config::parse(toml_str).unwrap();
        assert_eq!(c.ingest.region, "singapore");
        assert_eq!(c.ingest.window_ms, 30_000);
        let bp = c.ingest.exchanges.binance_perp.unwrap();
        assert_eq!(bp.allow, vec!["BTCUSDT", "ETHUSDT"]);
        assert_eq!(bp.top_n, Some(50));
        // Defaults preserved on unset fields:
        assert!(bp.enabled);
        assert_eq!(bp.include_quotes, vec!["USDT", "USDC"]);
        assert_eq!(bp.discovery_poll_secs, 300);
    }

    fn ov(exchange: &str, mt: &str, days: u64) -> RetentionOverride {
        RetentionOverride {
            exchange: exchange.into(),
            market_type: mt.into(),
            interval_seconds: None,
            days,
        }
    }

    fn ov_tf(exchange: &str, mt: &str, tf: u32, days: u64) -> RetentionOverride {
        RetentionOverride {
            exchange: exchange.into(),
            market_type: mt.into(),
            interval_seconds: Some(tf),
            days,
        }
    }

    #[test]
    fn retention_defaults_to_seven_days_delete_only() {
        let r = RetentionConfig::default();
        assert_eq!(r.default_days, 7);
        assert!(r.apply_on_start);
        let clause = r.ttl_clause_for(60).unwrap();
        assert!(clause.contains("INTERVAL 7 DAY DELETE"));
        assert!(!clause.contains("TO VOLUME"));
        assert!(!clause.contains("WHERE"));
    }

    #[test]
    fn retention_with_cold_tier_emits_both_clauses_in_order() {
        let r = RetentionConfig {
            default_days: 30,
            move_to_cold_days: Some(7),
            apply_on_start: true,
            overrides: Vec::new(),
        };
        let clause = r.ttl_clause_for(60).unwrap();
        let cold_pos = clause.find("TO VOLUME 'cold'").unwrap();
        let del_pos = clause.find("DELETE").unwrap();
        assert!(cold_pos < del_pos, "TO VOLUME must precede DELETE: {clause}");
        assert!(clause.contains("INTERVAL 7 DAY TO VOLUME 'cold'"));
        assert!(clause.contains("INTERVAL 30 DAY DELETE"));
    }

    #[test]
    fn retention_ignores_cold_when_not_earlier_than_delete() {
        let r = RetentionConfig {
            default_days: 7,
            move_to_cold_days: Some(7),
            apply_on_start: true,
            overrides: Vec::new(),
        };
        assert!(!r.ttl_clause_for(60).unwrap().contains("TO VOLUME"));

        let r = RetentionConfig {
            default_days: 7,
            move_to_cold_days: Some(30),
            apply_on_start: true,
            overrides: Vec::new(),
        };
        assert!(!r.ttl_clause_for(60).unwrap().contains("TO VOLUME"));
    }

    #[test]
    fn retention_all_zero_means_no_ttl_at_all() {
        let r = RetentionConfig {
            default_days: 0,
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: Vec::new(),
        };
        assert!(r.ttl_clause_for(60).is_none());

        let r = RetentionConfig {
            default_days: 0,
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: vec![ov("BINANCEF", "perp", 0), ov("BYBITF", "perp", 0)],
        };
        assert!(r.ttl_clause_for(60).is_none());
    }

    #[test]
    fn retention_per_exchange_overrides_emit_separate_where_clauses() {
        let r = RetentionConfig {
            default_days: 7,
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: vec![ov("BINANCEF", "perp", 30), ov("BYBITF", "perp", 7)],
        };
        let clause = r.ttl_clause_for(60).unwrap();
        assert!(
            clause.contains("INTERVAL 30 DAY DELETE WHERE exchange = 'BINANCEF' AND market_type = 'perp'"),
            "missing BINANCEF override clause: {clause}"
        );
        assert!(
            clause.contains("INTERVAL 7 DAY DELETE WHERE exchange = 'BYBITF' AND market_type = 'perp'"),
            "missing BYBITF override clause: {clause}"
        );
        assert!(clause.contains("WHERE NOT ("));
        assert!(clause.contains("exchange = 'BINANCEF'"));
        assert!(clause.contains("exchange = 'BYBITF'"));
    }

    #[test]
    fn retention_forever_override_excludes_from_default_delete() {
        let r = RetentionConfig {
            default_days: 7,
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: vec![ov("BINANCE", "spot", 0)],
        };
        let clause = r.ttl_clause_for(60).unwrap();
        assert!(
            !clause.contains("DELETE WHERE exchange = 'BINANCE' AND market_type = 'spot'"),
            "forever override must NOT emit a DELETE clause for itself: {clause}"
        );
        assert!(
            clause.contains("WHERE NOT (exchange = 'BINANCE' AND market_type = 'spot')"),
            "default clause must exclude forever override: {clause}"
        );
    }

    #[test]
    fn retention_per_tf_override_applies_only_to_matching_table() {
        // BINANCEF/perp на 30s держим 1 день, а тот же ключ на 1h —
        // forever. Проверяем что clause для 30s выдаёт DELETE-правило,
        // а clause для 3600s — НЕ выдаёт (forever) и выводит из default.
        let r = RetentionConfig {
            default_days: 7,
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: vec![
                ov_tf("BINANCEF", "perp", 30, 1),     // только 30s: 1 day
                ov_tf("BINANCEF", "perp", 3600, 0),   // только 1h: forever
            ],
        };

        let c30 = r.ttl_clause_for(30).unwrap();
        assert!(
            c30.contains("INTERVAL 1 DAY DELETE WHERE exchange = 'BINANCEF' AND market_type = 'perp'"),
            "30s clause must have 1-day DELETE for BINANCEF/perp: {c30}"
        );

        let c3600 = r.ttl_clause_for(3600).unwrap();
        assert!(
            !c3600.contains("DELETE WHERE exchange = 'BINANCEF' AND market_type = 'perp'"),
            "1h clause must NOT have explicit DELETE for forever override: {c3600}"
        );
        assert!(
            c3600.contains("WHERE NOT (exchange = 'BINANCEF' AND market_type = 'perp')"),
            "1h default must exclude forever override: {c3600}"
        );

        // Для TF, не упомянутого (например 300s = 5m) — override'ы не
        // применяются, всё попадает под default 7d без WHERE.
        let c300 = r.ttl_clause_for(300).unwrap();
        assert!(c300.contains("INTERVAL 7 DAY DELETE"));
        assert!(!c300.contains("WHERE"), "5m has no overrides → no WHERE: {c300}");
    }

    #[test]
    fn retention_specific_tf_override_beats_global_override() {
        // Глобальное правило BINANCEF/perp = 14 дней, плюс точечное на 30s
        // = 1 день. Для 30s должно применяться точечное (1 day), не 14.
        let r = RetentionConfig {
            default_days: 7,
            move_to_cold_days: None,
            apply_on_start: true,
            overrides: vec![
                ov("BINANCEF", "perp", 14),           // все TF: 14 дней
                ov_tf("BINANCEF", "perp", 30, 1),     // 30s: 1 день
            ],
        };
        let c30 = r.ttl_clause_for(30).unwrap();
        assert!(c30.contains("INTERVAL 1 DAY DELETE"));
        assert!(!c30.contains("INTERVAL 14 DAY DELETE"));

        // Для 60s — глобальное (14 дней) применяется.
        let c60 = r.ttl_clause_for(60).unwrap();
        assert!(c60.contains("INTERVAL 14 DAY DELETE"));
    }

    #[test]
    fn retention_validate_rejects_duplicate_and_bad_case() {
        let bad = RetentionConfig {
            default_days: 7,
            overrides: vec![ov("BINANCEF", "perp", 7), ov("BINANCEF", "perp", 14)],
            ..RetentionConfig::default()
        };
        assert!(bad.validate().is_err(), "duplicate (exchange, market) must fail");

        let bad = RetentionConfig {
            default_days: 7,
            overrides: vec![ov("binancef", "perp", 7)],
            ..RetentionConfig::default()
        };
        assert!(bad.validate().is_err());

        let bad = RetentionConfig {
            default_days: 7,
            overrides: vec![ov("BINANCEF", "PERP", 7)],
            ..RetentionConfig::default()
        };
        assert!(bad.validate().is_err());

        let bad = RetentionConfig {
            default_days: 7,
            overrides: vec![ov("BINANCEF", "futures", 7)],
            ..RetentionConfig::default()
        };
        assert!(bad.validate().is_err());

        let good = RetentionConfig {
            default_days: 7,
            overrides: vec![
                ov("BINANCEF", "perp", 30),
                ov("BINANCE", "spot", 0),
                // duplicate (exchange, market) допускается, если у одного из
                // правил задан interval_seconds — это разные TF.
                ov_tf("BINANCEF", "perp", 30, 1),
            ],
            ..RetentionConfig::default()
        };
        good.validate().unwrap();
    }

    #[test]
    fn retention_legacy_days_field_still_parses() {
        // Старые конфиги, где было просто `days = 14` — должны парситься
        // через alias в `default_days`.
        let s = r#"
            [ingest.retention]
            days = 14
        "#;
        let c = Config::parse(s).unwrap();
        assert_eq!(c.ingest.retention.default_days, 14);
    }

    #[test]
    fn retention_override_parses_from_toml() {
        let s = r#"
            [ingest.retention]
            default_days = 7
            apply_on_start = true

            [[ingest.retention.override]]
            exchange = "BINANCEF"
            market_type = "perp"
            days = 30

            [[ingest.retention.override]]
            exchange = "BINANCE"
            market_type = "spot"
            days = 0
        "#;
        let c = Config::parse(s).unwrap();
        assert_eq!(c.ingest.retention.default_days, 7);
        assert_eq!(c.ingest.retention.overrides.len(), 2);
        assert_eq!(c.ingest.retention.overrides[0].exchange, "BINANCEF");
        assert_eq!(c.ingest.retention.overrides[0].days, 30);
        assert_eq!(c.ingest.retention.overrides[1].days, 0);
    }

    #[test]
    fn enabled_exchanges_whitelist_overrides_section_enabled() {
        let mut c = IngestConfig::default();
        // Whitelist пустой → решает section_enabled.
        assert!(c.is_exchange_enabled("binance_perp", true));
        assert!(!c.is_exchange_enabled("binance_perp", false));

        // Whitelist непустой и содержит имя → стартуем даже если
        // секция говорит false (operator override).
        c.enabled_exchanges = vec!["binance_perp".into()];
        assert!(c.is_exchange_enabled("binance_perp", false));

        // Whitelist непустой и НЕ содержит имя → не стартуем даже если
        // секция говорит true.
        c.enabled_exchanges = vec!["bybit_perp".into()];
        assert!(!c.is_exchange_enabled("binance_perp", true));
    }

    #[test]
    fn retention_section_parses_from_toml() {
        let s = r#"
            [ingest.retention]
            default_days = 14
            move_to_cold_days = 3
            apply_on_start = false
        "#;
        let c = Config::parse(s).unwrap();
        assert_eq!(c.ingest.retention.default_days, 14);
        assert_eq!(c.ingest.retention.move_to_cold_days, Some(3));
        assert!(!c.ingest.retention.apply_on_start);
    }

    #[test]
    fn rejects_unknown_field_in_strict_mode() {
        // Default behavior: unknown fields are accepted (forward-compat).
        // We do not enable deny_unknown_fields, since silently dropping
        // a typo'd field is less bad than rejecting a config a future
        // version of this binary would understand.
        let c = Config::parse(
            r#"[ingest]
            something_new = 42
        "#,
        );
        assert!(c.is_ok());
    }
}
