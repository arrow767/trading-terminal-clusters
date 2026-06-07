//! Локальная копия price-scale helper'ов — те же что в `exchange-bybit`/
//! `exchange-okx`, без cross-crate зависимости (биржевые adapter'ы
//! независимы друг от друга).
//!
//! Конвенция scale идентична остальным адаптерам: `parse_scaled` парсит
//! decimal-строку в i64 при заданном числе decimal-знаков, так что
//! terminal-local и server prices согласованы на уровне int64 raw-значения.

use exchange_core::{ExchangeError, Result};

// Bitget REST gives integer precision counts directly, so this isn't used by
// the instruments parser (unlike Bybit/OKX) — kept for parity + the unit test.
#[allow(dead_code)]
pub(crate) fn count_decimals_trimmed(value: &str) -> u8 {
    let Some(dot) = value.find('.') else {
        return 0;
    };
    let trimmed = value.trim_end_matches('0');
    if trimmed.ends_with('.') {
        return 0;
    }
    let after_dot = trimmed.len() - dot - 1;
    after_dot.min(255) as u8
}

/// Парс decimal-строки в i64 с заданным `scale` (количество decimal-знаков).
/// Если scale=0 и есть дробная часть — она усекается целиком.
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

    let frac_val: i64 = if frac_part.is_empty() || scale_usize == 0 {
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
    fn decimals_and_scale_match_convention() {
        assert_eq!(count_decimals_trimmed("0.10"), 1);
        assert_eq!(count_decimals_trimmed("0.001"), 3);
        assert_eq!(count_decimals_trimmed("1"), 0);
        assert_eq!(parse_scaled("67234.50", 2).unwrap(), 6_723_450);
        assert_eq!(parse_scaled("100", 0).unwrap(), 100);
        assert_eq!(parse_scaled("0.005", 3).unwrap(), 5);
    }
}
