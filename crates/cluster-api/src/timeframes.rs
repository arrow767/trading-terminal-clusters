//! Канонический список таймфреймов и их CH-имён таблиц.
//!
//! Раньше эта логика жила в `cluster-ingest::config`. Перенесена сюда,
//! потому что её используют сразу два места: ingest (где брать `ch_tx`
//! для каждого TF) и REST-handler `GET /v1/clusters/range` (в какую
//! таблицу `SELECT`). Один источник правды → нет шанса разойтись по
//! schemas.
//!
//! Любое добавление поддерживаемого TF — здесь + соответствующая
//! миграция в `migrations/003_timeframes.sql`.

/// Список TF, для которых есть готовые таблицы в миграции 003.
/// Запросы на другие значения должны быть отклонены validate_interval.
pub const SUPPORTED_INTERVALS: &[u32] = &[30, 60, 300, 900, 1800, 3600, 14400, 86400];

/// Имя таблицы CH для interval_seconds. Convention: `clusters_<human>`,
/// где `<human>` = 30s / 1m / 5m / 15m / 1h / 4h / 1d для стандартных
/// значений. Для произвольных (не из SUPPORTED_INTERVALS) формат
/// `clusters_<N>s` — но таких таблиц в миграции нет, так что fallback
/// существует только для тестов и валидации сообщений об ошибках.
pub fn table_name_for(interval_seconds: u32) -> String {
    let human = match interval_seconds {
        30 => "30s".to_string(),
        60 => "1m".to_string(),
        300 => "5m".to_string(),
        900 => "15m".to_string(),
        1800 => "30m".to_string(),
        3600 => "1h".to_string(),
        14400 => "4h".to_string(),
        86400 => "1d".to_string(),
        n if n % 86400 == 0 => format!("{}d", n / 86400),
        n if n % 3600 == 0 => format!("{}h", n / 3600),
        n if n % 60 == 0 => format!("{}m", n / 60),
        n => format!("{n}s"),
    };
    format!("clusters_{human}")
}

/// True если interval имеет соответствующую таблицу в миграциях.
pub fn is_supported_interval(interval_seconds: u32) -> bool {
    SUPPORTED_INTERVALS.contains(&interval_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_intervals_map_to_human_names() {
        assert_eq!(table_name_for(30), "clusters_30s");
        assert_eq!(table_name_for(60), "clusters_1m");
        assert_eq!(table_name_for(300), "clusters_5m");
        assert_eq!(table_name_for(900), "clusters_15m");
        assert_eq!(table_name_for(1800), "clusters_30m");
        assert_eq!(table_name_for(3600), "clusters_1h");
        assert_eq!(table_name_for(14400), "clusters_4h");
        assert_eq!(table_name_for(86400), "clusters_1d");
    }

    #[test]
    fn non_standard_falls_back_to_unit_format() {
        assert_eq!(table_name_for(45), "clusters_45s");
        assert_eq!(table_name_for(120), "clusters_2m");
        assert_eq!(table_name_for(7200), "clusters_2h");
        assert_eq!(table_name_for(172800), "clusters_2d");
    }

    #[test]
    fn supported_intervals_all_resolve_and_validate() {
        for &iv in SUPPORTED_INTERVALS {
            assert!(is_supported_interval(iv), "{iv} should be supported");
            assert!(table_name_for(iv).starts_with("clusters_"));
        }
        assert!(!is_supported_interval(45));
        assert!(!is_supported_interval(7200));
    }
}
