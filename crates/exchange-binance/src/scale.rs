use exchange_core::{ExchangeError, Result};

/// Считает значимые десятичные знаки в строковом представлении decimal'а:
/// "0.10000000" → 1; "0.01" → 2; "1" → 0; "0.123" → 3.
///
/// КРИТИЧНО для consistency: fat-terminal (TradingTerminal.Engine.Specs.cs)
/// использует ровно эту формулу для вычисления своего price_scale/qty_scale
/// от tickSize/stepSize. Если сервер возьмёт `pricePrecision` из exchangeInfo
/// (которое для BTCUSDT futures = 2, а tickSize = "0.10" → decimals = 1)
/// — scale разойдётся на 10×, история на чарте будет на y-coord в 10 раз
/// выше реального. Для spot ещё хуже: `quoteAssetPrecision = 8` всегда,
/// расхождение до 10^6.
///
/// Единственный источник правды — tickSize/stepSize string. Здесь.
pub(crate) fn count_decimals_trimmed(value: &str) -> u8 {
    let Some(dot) = value.find('.') else {
        return 0;
    };
    let trimmed = value.trim_end_matches('0');
    // "1." → 0; "1.0" trimmed → "1." → 0
    if trimmed.ends_with('.') {
        return 0;
    }
    let after_dot = trimmed.len() - dot - 1;
    after_dot.min(255) as u8
}

/// Convert a Binance decimal string like "0.10" to a scaled `i64`,
/// where the result equals `round(value * 10^scale)`.
///
/// Binance returns prices and quantities as strings with a precision
/// declared elsewhere in the exchangeInfo response. We pre-scale at
/// parse time so the rest of the pipeline is integer-only and free of
/// `f64` rounding hazards.
///
/// If the fractional part has more digits than `scale`, the surplus is
/// truncated (Binance does not currently emit such values, but the
/// behavior is well-defined rather than panicking). Negative numbers
/// are not expected and rejected.
pub(crate) fn parse_scaled(s: &str, scale: u8) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ExchangeError::Parse("empty number".into()));
    }
    if s.starts_with('-') {
        return Err(ExchangeError::Parse(format!(
            "negative number not allowed: {s}"
        )));
    }

    let mut parts = s.splitn(2, '.');
    let int_part = parts.next().unwrap_or("0");
    let frac_part = parts.next().unwrap_or("");

    if int_part.is_empty() || !int_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(ExchangeError::Parse(format!("invalid integer part: {s}")));
    }
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(ExchangeError::Parse(format!(
            "invalid fractional part: {s}"
        )));
    }

    let scale_usize = scale as usize;
    let int_val: i64 = int_part
        .parse()
        .map_err(|e: std::num::ParseIntError| ExchangeError::Parse(e.to_string()))?;

    let frac_val: i64 = if frac_part.is_empty() {
        0
    } else if frac_part.len() <= scale_usize {
        let mut padded = String::with_capacity(scale_usize);
        padded.push_str(frac_part);
        for _ in frac_part.len()..scale_usize {
            padded.push('0');
        }
        padded
            .parse()
            .map_err(|e: std::num::ParseIntError| ExchangeError::Parse(e.to_string()))?
    } else {
        frac_part[..scale_usize]
            .parse()
            .map_err(|e: std::num::ParseIntError| ExchangeError::Parse(e.to_string()))?
    };

    let multiplier: i64 = 10i64
        .checked_pow(scale as u32)
        .ok_or_else(|| ExchangeError::Parse(format!("scale {scale} overflows i64")))?;
    int_val
        .checked_mul(multiplier)
        .and_then(|v| v.checked_add(frac_val))
        .ok_or_else(|| ExchangeError::Parse(format!("value {s} overflows i64 at scale {scale}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn integer_only() {
        assert_eq!(parse_scaled("1", 2).unwrap(), 100);
        assert_eq!(parse_scaled("123", 0).unwrap(), 123);
    }

    #[test]
    fn fractional_padding() {
        assert_eq!(parse_scaled("0.10", 2).unwrap(), 10);
        assert_eq!(parse_scaled("1.5", 3).unwrap(), 1500);
    }

    #[test]
    fn fractional_truncation() {
        // 0.123 truncated to scale 2 → 0.12 → 12
        assert_eq!(parse_scaled("0.123", 2).unwrap(), 12);
    }

    #[test]
    fn binance_typical_price() {
        // BTCUSDT price "67234.50" with scale=2 → 6_723_450
        assert_eq!(parse_scaled("67234.50", 2).unwrap(), 6_723_450);
    }

    #[test]
    fn rejects_negative() {
        assert!(parse_scaled("-1.0", 2).is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_scaled("", 2).is_err());
        assert!(parse_scaled("abc", 2).is_err());
        assert!(parse_scaled("1.2.3", 2).is_err());
    }

    #[test]
    fn count_decimals_trimmed_matches_terminal_convention() {
        // Из EngineServer.Specs.cs:CountDecimalsTrimmed — это конвенция
        // fat-terminal'а. Должны совпадать байт-в-байт.
        assert_eq!(count_decimals_trimmed("0.10000000"), 1); // BTCUSDT futures tick
        assert_eq!(count_decimals_trimmed("0.01000000"), 2); // ETHUSDT spot tick
        assert_eq!(count_decimals_trimmed("0.10"), 1);
        assert_eq!(count_decimals_trimmed("0.01"), 2);
        assert_eq!(count_decimals_trimmed("0.001"), 3);
        assert_eq!(count_decimals_trimmed("1"), 0);
        assert_eq!(count_decimals_trimmed("1.0"), 0);
        assert_eq!(count_decimals_trimmed("100"), 0);
        assert_eq!(count_decimals_trimmed("0.0001"), 4);
    }
}
