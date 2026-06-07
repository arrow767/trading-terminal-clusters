//! Scale helper + native-coin map for Hyperliquid.
//!
//! HL has no fixed tick; the live engine derives price precision from mid+nSigFigs
//! (dynamic). Per the prod decision we instead pin the MAX precision per symbol:
//! `price_scale = MAX_DECIMALS - szDecimals` (MAX_DECIMALS = 6 perp), `qty_scale =
//! szDecimals`. This is the finest grid HL allows, fixed from `meta` — so the
//! terminal can downscale server history to its current live scale losslessly.
//!
//! The wire `coin` ("BTC", "kPEPE", "HYPE") is case-sensitive and isn't always
//! recoverable from the canonical symbol (k-prefix coins), so we keep a
//! canonical→native map populated at discovery.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

use exchange_core::{ExchangeError, Result};

/// Max price decimal places for perps (HL rule: <= MAX_DECIMALS - szDecimals).
pub(crate) const MAX_DECIMALS_PERP: u8 = 6;

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

// ─── canonical (BTCUSDC) → native wire coin (BTC / kPEPE) ─────────────────────
static NATIVE_MAP: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub(crate) fn set_native(canonical: &str, native: &str) {
    if let Ok(mut m) = NATIVE_MAP.write() {
        m.insert(canonical.to_string(), native.to_string());
    }
}

/// Native wire coin for a canonical symbol. Falls back to suffix-strip
/// (`BTCUSDC`→`BTC`) when the map is cold.
pub fn get_native(canonical: &str) -> String {
    if let Some(n) = NATIVE_MAP.read().ok().and_then(|m| m.get(canonical).cloned()) {
        return n;
    }
    let up = canonical.to_uppercase();
    up.strip_suffix("USDC").unwrap_or(&up).to_string()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn scaled_and_native() {
        assert_eq!(parse_scaled("68960.5", 1).unwrap(), 689605);
        assert_eq!(parse_scaled("0.0123", 4).unwrap(), 123);
        set_native("KPEPEUSDC", "kPEPE");
        assert_eq!(get_native("KPEPEUSDC"), "kPEPE");
        assert_eq!(get_native("BTCUSDC"), "BTC"); // fallback strip
    }
}
