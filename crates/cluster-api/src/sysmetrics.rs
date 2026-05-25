//! GET /v1/system/metrics — CPU / RAM / disk / load + uptime, плюс
//! per-table breakdown ClickHouse. Эндпоинт защищён тем же bearer
//! middleware, что и data-роуты; cloud admin проксирует его, прячет
//! токен сервер-сайдом, и рендерит карточку.
//!
//! CPU usage — это дельта-измерение: sysinfo требует двух семплов
//! с интервалом, поэтому делаем refresh → sleep 200мс → refresh. Пик
//! с момента запуска процесса хранится в атомике (centi-percent) —
//! страница показывает «peak since boot» без своей истории.

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use sysinfo::{Disks, System};

/// Peak global CPU %, ×100 чтобы влезало в u32. Monotonic за время
/// жизни процесса; сбрасывается только рестартом.
static CPU_PEAK_CENTI: AtomicU32 = AtomicU32::new(0);

/// Состояние, нужное хендлеру: HTTP-клиент ClickHouse (для per-table
/// breakdown) + параметры подключения. Если `ch_client` отсутствует
/// (e.g. в unit-тестах) → таблицы возвращаются пустым списком, OS-уровень
/// диска всё равно отрисуется.
#[derive(Clone)]
pub struct SysMetricsState {
    pub ch_client: Option<reqwest::Client>,
    pub ch_url: String,
    pub ch_database: String,
}

#[derive(Debug, Serialize)]
pub struct SystemMetrics {
    hostname: String,
    uptime_secs: u64,
    cpu: CpuMetrics,
    mem: MemMetrics,
    disk: DiskMetrics,
    /// Per-table ClickHouse on-disk breakdown ("что съело диск"). Пустой
    /// если CH-клиент не настроен или запрос упал — страница не уйдёт
    /// в blank, OS-disk бар останется.
    tables: Vec<TableUsage>,
}

#[derive(Debug, Serialize)]
struct TableUsage {
    table: String,
    rows: u64,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

#[derive(Debug, Serialize)]
struct CpuMetrics {
    cores: usize,
    usage_pct: f32,
    /// Наибольший usage_pct, замеченный с момента старта api-процесса.
    peak_pct: f32,
    load1: f64,
    load5: f64,
    load15: f64,
}

#[derive(Debug, Serialize)]
struct MemMetrics {
    total_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
    used_pct: f32,
}

#[derive(Debug, Serialize)]
struct DiskMetrics {
    /// Точка монтирования, которую мы измерили (mount с максимальным
    /// total — обычно тот же NVMe, где живёт ClickHouse data + WAL,
    /// то что мы реально боимся переполнить).
    mount: String,
    total_bytes: u64,
    used_bytes: u64,
    free_bytes: u64,
    used_pct: f32,
}

/// GET /v1/system/metrics — обработчик. Auth применяется выше через
/// общий middleware; здесь только сбор данных.
pub async fn system_metrics(State(state): State<SysMetricsState>) -> impl IntoResponse {
    let tables = fetch_table_sizes(&state).await.unwrap_or_default();

    let mut sys = System::new();
    // CPU delta: первый семпл, sleep 200мс, второй семпл. 200мс выше
    // sysinfo MINIMUM_CPU_UPDATE_INTERVAL и не превращает запрос в долгий.
    sys.refresh_cpu_usage();
    tokio::time::sleep(Duration::from_millis(200)).await;
    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let usage = sys.global_cpu_usage();
    let centi = (usage * 100.0).round() as u32;
    let mut prev = CPU_PEAK_CENTI.load(Ordering::Relaxed);
    while centi > prev {
        match CPU_PEAK_CENTI.compare_exchange_weak(
            prev,
            centi,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(p) => prev = p,
        }
    }
    let peak_pct = CPU_PEAK_CENTI.load(Ordering::Relaxed) as f32 / 100.0;

    let load = System::load_average();
    let total_mem = sys.total_memory();
    let avail_mem = sys.available_memory();
    let used_mem = total_mem.saturating_sub(avail_mem);
    let mem_pct = if total_mem > 0 {
        used_mem as f32 / total_mem as f32 * 100.0
    } else {
        0.0
    };

    // Disk: берём mount с максимальным total — это и есть NVMe-том с CH.
    let disks = Disks::new_with_refreshed_list();
    let mut best: Option<DiskMetrics> = None;
    for d in disks.list() {
        let total = d.total_space();
        let free = d.available_space();
        let used = total.saturating_sub(free);
        let cand = DiskMetrics {
            mount: d.mount_point().to_string_lossy().into_owned(),
            total_bytes: total,
            used_bytes: used,
            free_bytes: free,
            used_pct: if total > 0 {
                used as f32 / total as f32 * 100.0
            } else {
                0.0
            },
        };
        if best.as_ref().map_or(true, |b| cand.total_bytes > b.total_bytes) {
            best = Some(cand);
        }
    }
    let disk = best.unwrap_or(DiskMetrics {
        mount: "unknown".into(),
        total_bytes: 0,
        used_bytes: 0,
        free_bytes: 0,
        used_pct: 0.0,
    });

    let body = SystemMetrics {
        hostname: System::host_name().unwrap_or_else(|| "unknown".into()),
        uptime_secs: System::uptime(),
        cpu: CpuMetrics {
            cores: num_cpus(&sys),
            usage_pct: usage,
            peak_pct,
            load1: load.one,
            load5: load.five,
            load15: load.fifteen,
        },
        mem: MemMetrics {
            total_bytes: total_mem,
            used_bytes: used_mem,
            available_bytes: avail_mem,
            used_pct: mem_pct,
        },
        disk,
        tables,
    };
    (StatusCode::OK, Json(body))
}

/// Запросить из system.parts общий on-disk размер каждой таблицы в нашей
/// базе. Результат отсортирован по compressed_bytes убыванию, чтобы
/// «съедающие диск» таблицы были сверху. Любая ошибка → пустой Vec
/// (страница рендерится без таблиц, OS-диск-бар остаётся).
async fn fetch_table_sizes(state: &SysMetricsState) -> Option<Vec<TableUsage>> {
    let client = state.ch_client.as_ref()?;
    // FORMAT JSONEachRow возвращает по объекту на строку — парсим
    // построчно, чтобы не тащить лишних зависимостей.
    let sql = format!(
        "SELECT table, sum(rows) AS rows, sum(bytes_on_disk) AS compressed_bytes, \
         sum(data_uncompressed_bytes) AS uncompressed_bytes \
         FROM system.parts WHERE active AND database = '{}' \
         GROUP BY table ORDER BY compressed_bytes DESC FORMAT JSONEachRow",
        state.ch_database.replace('\'', "''")
    );
    let url = format!("{}/?query={}", state.ch_url.trim_end_matches('/'), urlencode(&sql));
    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body = resp.text().await.ok()?;
    let mut out = Vec::new();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // CH возвращает строковые числа в JSONEachRow для UInt64, и
        // числовые для меньших типов — десериализуем мягко.
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let table = v.get("table").and_then(|x| x.as_str())?.to_string();
        let rows = json_u64(v.get("rows"))?;
        let comp = json_u64(v.get("compressed_bytes"))?;
        let uncomp = json_u64(v.get("uncompressed_bytes"))?;
        out.push(TableUsage {
            table,
            rows,
            compressed_bytes: comp,
            uncompressed_bytes: uncomp,
        });
    }
    Some(out)
}

fn json_u64(v: Option<&serde_json::Value>) -> Option<u64> {
    match v? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
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

fn num_cpus(sys: &System) -> usize {
    let n = sys.cpus().len();
    if n == 0 {
        1
    } else {
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_u64_handles_both_string_and_number() {
        let n = serde_json::json!(42);
        let s = serde_json::json!("42");
        assert_eq!(json_u64(Some(&n)), Some(42));
        assert_eq!(json_u64(Some(&s)), Some(42));
        assert_eq!(json_u64(Some(&serde_json::json!(null))), None);
        assert_eq!(json_u64(None), None);
    }

    #[test]
    fn urlencode_handles_quotes_and_punct() {
        let q = "WHERE database = 'clusters'";
        let enc = urlencode(q);
        // Одинарная кавычка должна быть закодирована.
        assert!(enc.contains("%27"));
        // Простые буквы — нет.
        assert!(enc.contains("WHERE"));
    }
}
