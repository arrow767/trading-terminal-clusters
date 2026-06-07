//! Scale/decimal helpers + symbol mapping + per-symbol contract multiplier
//! (futures), ported from the live engine. Spot canonical = `BTCUSDT`;
//! futures venue = `BTC_USDT` (underscore). Futures qty is in CONTRACTS →
//! base via `contractSize` fraction (like OKX); spot is already in base.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use exchange_core::{ExchangeError, Result};

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

pub(crate) fn parse_scaled(s: &str, scale: u8) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ExchangeError::Parse("empty number".into()));
    }
    if s.starts_with('-') {
        return Err(ExchangeError::Parse(format!("negative number not allowed: {s}")));
    }
    let mut parts = s.splitn(2, '.');
    let int_part = parts.next().unwrap_or("0");
    let frac_part = parts.next().unwrap_or("");
    if int_part.is_empty() || !int_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(ExchangeError::Parse(format!("invalid integer part: {s}")));
    }
    if !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(ExchangeError::Parse(format!("invalid fractional part: {s}")));
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

pub(crate) fn multiply_decimal_strs(a: &str, b: &str) -> String {
    let dec_a = a.find('.').map(|d| a.len() - d - 1).unwrap_or(0);
    let dec_b = b.find('.').map(|d| b.len() - d - 1).unwrap_or(0);
    let total_dec = dec_a + dec_b;
    let int_a: i128 = a.replace('.', "").parse().unwrap_or(1);
    let int_b: i128 = b.replace('.', "").parse().unwrap_or(1);
    let product = int_a * int_b;
    if total_dec == 0 {
        return product.to_string();
    }
    let divisor = 10_i128.pow(total_dec as u32);
    let int_part = product / divisor;
    let frac_part = (product % divisor).abs();
    if frac_part == 0 {
        return int_part.to_string();
    }
    let frac_str = format!("{frac_part:0>width$}", width = total_dec);
    let frac_str = frac_str.trim_end_matches('0');
    format!("{int_part}.{frac_str}")
}

pub(crate) fn decimal_fraction(s: &str) -> (i64, i64) {
    let decimals = s.find('.').map(|d| s.len() - d - 1).unwrap_or(0);
    let denom = 10_i64.pow(decimals as u32);
    let num_str: String = s.chars().filter(|c| *c != '.').collect();
    let num = num_str.parse::<i64>().unwrap_or(1);
    let g = gcd(num.max(1), denom);
    (num / g, denom / g)
}

fn gcd(mut a: i64, mut b: i64) -> i64 {
    a = a.abs();
    b = b.abs();
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a.max(1)
}

/// f64 from contract detail → clean decimal string (0.0001→"0.0001", 1.0→"1").
pub(crate) fn fmt_decimal(v: f64, default: &str) -> String {
    if v.is_finite() && v > 0.0 {
        format!("{v}")
    } else {
        default.to_owned()
    }
}

/// Canonical `BTCUSDT` → MEXC contract venue symbol `BTC_USDT`.
pub fn to_mexc_contract_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    if upper.contains('_') {
        return upper;
    }
    let (base, quote) = if upper.ends_with("USDT") {
        (&upper[..upper.len() - 4], "USDT")
    } else if upper.ends_with("USDC") {
        (&upper[..upper.len() - 4], "USDC")
    } else {
        (&upper[..upper.len() - 4], "USDT")
    };
    format!("{base}_{quote}")
}

// ─── Per-symbol contract multiplier (futures only), keyed by canonical ────────
static CT_MAP: LazyLock<RwLock<HashMap<String, (i64, i64)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub(crate) fn set_ct(symbol: &str, num: i64, den: i64) {
    if let Ok(mut m) = CT_MAP.write() {
        m.insert(symbol.to_string(), (num, den));
    }
}

pub(crate) fn get_ct(symbol: &str) -> (i64, i64) {
    CT_MAP
        .read()
        .ok()
        .and_then(|m| m.get(symbol).copied())
        .unwrap_or((1, 1))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert_eq!(count_decimals_trimmed("0.000001"), 6);
        assert_eq!(parse_scaled("100.5", 2).unwrap(), 10050);
        assert_eq!(multiply_decimal_strs("1", "0.0001"), "0.0001");
        assert_eq!(decimal_fraction("0.0001"), (1, 10000));
        assert_eq!(to_mexc_contract_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(to_mexc_contract_symbol("ETHUSDC"), "ETH_USDC");
        assert_eq!(fmt_decimal(0.0001, "1"), "0.0001");
    }
}
