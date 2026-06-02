//! Scale / decimal helpers + OKX instId<->canonical symbol mapping + the
//! per-symbol contract-multiplier table for swaps.
//!
//! `count_decimals_trimmed` / `parse_scaled` are the SAME convention every
//! adapter uses (see `exchange-bybit::scale`), so terminal-local and server
//! prices agree at the int64 raw level. The OKX-specific pieces
//! (`multiply_decimal_strs`, `ct_val_fraction`, `to_okx_inst_id`,
//! `normalize_inst_id`) are ported verbatim from the live engine
//! (`rust-ws-engine/src/okx/rest.rs`) so the server reproduces identical
//! price_scale / qty_scale / base-unit qty.

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

/// Парс decimal-строки в i64 с заданным `scale`. scale=0 → дробь усекается.
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

/// Convert canonical symbol (BTCUSDT) → OKX instId (BTC-USDT / BTC-USDT-SWAP).
/// Mirrors `rust-ws-engine::okx::rest::to_okx_inst_id`.
pub(crate) fn to_okx_inst_id(symbol: &str, is_swap: bool) -> String {
    let upper = symbol.to_uppercase();
    let (base, quote) = if upper.ends_with("USDT") {
        (&upper[..upper.len() - 4], "USDT")
    } else if upper.ends_with("USDC") {
        (&upper[..upper.len() - 4], "USDC")
    } else {
        (&upper[..upper.len() - 3], &upper[upper.len() - 3..])
    };
    if is_swap {
        format!("{base}-{quote}-SWAP")
    } else {
        format!("{base}-{quote}")
    }
}

/// OKX instId (BTC-USDT / BTC-USDT-SWAP) → canonical BTCUSDT.
/// Mirrors the live engine's `inst_id.replace("-SWAP","").replace('-',"").to_uppercase()`.
pub(crate) fn normalize_inst_id(inst_id: &str) -> String {
    inst_id.replace("-SWAP", "").replace('-', "").to_uppercase()
}

/// Multiply two decimal strings with integer math (no float error).
/// "1" * "0.01" = "0.01", "0.1" * "10" = "1". Ported from the live engine.
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

/// ctVal string → (num, den) reduced fraction. "0.1"→(1,10), "10"→(10,1),
/// "1"→(1,1). Ported from the live engine.
pub(crate) fn ct_val_fraction(ct_val: &str) -> (i64, i64) {
    let decimals = ct_val
        .find('.')
        .map(|dot| ct_val.len() - dot - 1)
        .unwrap_or(0);
    let denom = 10_i64.pow(decimals as u32);
    let num_str: String = ct_val.chars().filter(|c| *c != '.').collect();
    let num = num_str.parse::<i64>().unwrap_or(1);
    let g = gcd(num, denom);
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
    a
}

// ─── Per-symbol contract multiplier (swaps only) ──────────────────────────────
//
// `SymbolSpec` (exchange-core) carries no contract-size field, so we keep the
// swap qty multiplier in a process-local table keyed by canonical symbol. ONLY
// SWAP discovery writes it; the trade parser reads it ONLY for `MarketType::Perp`
// (so a same-named spot pair never picks up a swap's ctVal). Populated by
// `OkxInstrumentsInfo::fetch_symbols` before the WS session connects, so trades
// always see a ready value (absent → (1,1), i.e. no conversion).

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
    fn decimals_and_scale_match_convention() {
        assert_eq!(count_decimals_trimmed("0.10"), 1);
        assert_eq!(count_decimals_trimmed("0.001"), 3);
        assert_eq!(count_decimals_trimmed("1"), 0);
        assert_eq!(parse_scaled("67234.50", 2).unwrap(), 6_723_450);
        assert_eq!(parse_scaled("100", 0).unwrap(), 100);
    }

    #[test]
    fn inst_id_roundtrip() {
        assert_eq!(to_okx_inst_id("BTCUSDT", false), "BTC-USDT");
        assert_eq!(to_okx_inst_id("BTCUSDT", true), "BTC-USDT-SWAP");
        assert_eq!(to_okx_inst_id("ETHUSDC", true), "ETH-USDC-SWAP");
        assert_eq!(normalize_inst_id("BTC-USDT"), "BTCUSDT");
        assert_eq!(normalize_inst_id("BTC-USDT-SWAP"), "BTCUSDT");
        assert_eq!(normalize_inst_id("1000PEPE-USDT-SWAP"), "1000PEPEUSDT");
    }

    #[test]
    fn ctval_math_matches_live_engine() {
        // lotSz 1 * ctVal 0.001 → effective lot "0.001" (qty_scale 3),
        // fraction (1, 1000). 100 contracts → parse_scaled("100",3)=100000,
        // *1/1000 = 100 = 0.100 base @ scale 3 = 0.1 base.
        assert_eq!(multiply_decimal_strs("1", "0.001"), "0.001");
        assert_eq!(ct_val_fraction("0.001"), (1, 1000));
        assert_eq!(ct_val_fraction("0.1"), (1, 10));
        assert_eq!(ct_val_fraction("10"), (10, 1));
        assert_eq!(ct_val_fraction("1"), (1, 1));
        let qty = parse_scaled("100", 3).unwrap() * 1 / 1000;
        assert_eq!(qty, 100); // 0.1 base @ scale 3
    }

    #[test]
    fn ct_map_is_perp_scoped_via_caller() {
        set_ct("DOGEUSDT", 1000, 1);
        assert_eq!(get_ct("DOGEUSDT"), (1000, 1));
        assert_eq!(get_ct("NEVERSEENUSDT"), (1, 1));
    }
}
