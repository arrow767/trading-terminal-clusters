//! Scale/decimal helpers + KuCoin symbol mapping + per-symbol contract
//! multiplier table (futures). Ported from the live engine
//! (`rust-ws-engine/src/kucoin/rest.rs`) so server prices/qty match
//! byte-for-byte. KuCoin futures sizes are in CONTRACTS → base via the
//! `multiplier` fraction (like OKX ctVal); spot is already in base.

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

/// Multiply two decimal strings with integer math (no float error).
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

/// Decimal string → reduced (num, den): "0.001"→(1,1000), "10"→(10,1).
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

/// KuCoin base ccy → normalized base (XBT→BTC).
pub(crate) fn norm_base(base: &str) -> String {
    if base.eq_ignore_ascii_case("XBT") {
        "BTC".to_owned()
    } else {
        base.to_uppercase()
    }
}

/// Canonical `BTCUSDT` → KuCoin venue symbol.
/// Futures: `XBTUSDTM` (BTC→XBT, suffix M). Spot: `BTC-USDT`.
pub fn to_kucoin_symbol(symbol: &str, is_futures: bool) -> String {
    let upper = symbol.to_uppercase();
    let (base, quote) = if upper.ends_with("USDT") {
        (&upper[..upper.len() - 4], "USDT")
    } else if upper.ends_with("USDC") {
        (&upper[..upper.len() - 4], "USDC")
    } else {
        (&upper[..upper.len() - 3], &upper[upper.len() - 3..])
    };
    if is_futures {
        let base_f = if base == "BTC" { "XBT" } else { base };
        format!("{base_f}{quote}M")
    } else {
        format!("{base}-{quote}")
    }
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
    fn symbol_mapping() {
        assert_eq!(to_kucoin_symbol("BTCUSDT", true), "XBTUSDTM");
        assert_eq!(to_kucoin_symbol("ETHUSDT", true), "ETHUSDTM");
        assert_eq!(to_kucoin_symbol("ETHUSDC", true), "ETHUSDCM");
        assert_eq!(to_kucoin_symbol("BTCUSDT", false), "BTC-USDT");
        assert_eq!(norm_base("XBT"), "BTC");
        assert_eq!(norm_base("eth"), "ETH");
    }

    #[test]
    fn scale_and_fraction() {
        assert_eq!(count_decimals_trimmed("0.001"), 3);
        assert_eq!(parse_scaled("100.5", 2).unwrap(), 10050);
        assert_eq!(multiply_decimal_strs("1", "0.001"), "0.001");
        assert_eq!(decimal_fraction("0.001"), (1, 1000));
        assert_eq!(decimal_fraction("10"), (10, 1));
    }
}
