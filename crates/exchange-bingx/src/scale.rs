//! Scale/decimal helpers + symbol mapping for BingX, ported from the live engine
//! (`rust-ws-engine/src/bingx/rest.rs`). BingX perp qty is BASE-asset — there is
//! NO contract multiplier (ct = 1/1) — so no CT map here. Canonical `BTCUSDT`;
//! BingX wire symbol `BTC-USDT` (dash form, both spot and swap, no `-SWAP`).

use exchange_core::{ExchangeError, Result};

pub(crate) fn count_decimals_trimmed(value: &str) -> u8 {
    let Some(dot) = value.find('.') else {
        return 0;
    };
    let trimmed = value.trim_end_matches('0');
    if trimmed.ends_with('.') {
        return 0;
    }
    (trimmed.len() - dot - 1).min(255) as u8
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

/// f64 (spot tickSize/stepSize arrive as JSON numbers) → clean decimal string.
pub(crate) fn fmt_decimal(x: f64) -> String {
    if !x.is_finite() || x <= 0.0 {
        return "0".to_owned();
    }
    let s = format!("{x:.12}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_owned()
    } else {
        s.to_owned()
    }
}

/// Canonical `BTCUSDT` → BingX wire symbol `BTC-USDT` (dash form, no `-SWAP`).
pub fn to_bingx_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    let (base, quote) = if upper.ends_with("USDT") {
        (&upper[..upper.len() - 4], "USDT")
    } else if upper.ends_with("USDC") {
        (&upper[..upper.len() - 4], "USDC")
    } else if upper.len() > 3 {
        (&upper[..upper.len() - 3], &upper[upper.len() - 3..])
    } else {
        return upper;
    };
    format!("{base}-{quote}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert_eq!(count_decimals_trimmed("0.0001"), 4);
        assert_eq!(count_decimals_trimmed("1"), 0);
        assert_eq!(parse_scaled("100.5", 1).unwrap(), 1005);
        assert_eq!(parse_scaled("0.0001", 4).unwrap(), 1);
        assert_eq!(fmt_decimal(0.01), "0.01");
        assert_eq!(to_bingx_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(to_bingx_symbol("ETHUSDC"), "ETH-USDC");
    }
}
