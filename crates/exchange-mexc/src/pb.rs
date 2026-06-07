//! Minimal hand-rolled protobuf decoder for MEXC spot WS — DEALS only.
//!
//! MEXC spot v3 pushes market data only as protobuf: each binary frame is a
//! `PushDataV3ApiWrapper` whose `body` (a length-delimited field in the
//! 301..=399 range) is the message. We decode just the wrapper envelope +
//! `PublicAggreDealsV3Api`. Numbers stay as borrowed `&str` slices and are
//! scaled by the caller. Ported from the live engine (`rust-ws-engine/src/
//! mexc/pb.rs`). Wire types: 0=varint, 1=64-bit, 2=length-delimited, 5=32-bit.

struct Pb<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Pb<'a> {
    #[inline]
    fn new(buf: &'a [u8]) -> Self {
        Pb { buf, pos: 0 }
    }
    #[inline]
    fn at_end(&self) -> bool {
        self.pos >= self.buf.len()
    }
    #[inline]
    fn read_varint(&mut self) -> Option<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte = *self.buf.get(self.pos)?;
            self.pos += 1;
            result |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }
    #[inline]
    fn read_tag(&mut self) -> Option<(u32, u8)> {
        let key = self.read_varint()?;
        let field = (key >> 3) as u32;
        let wire = (key & 0x07) as u8;
        if field == 0 {
            return None;
        }
        Some((field, wire))
    }
    #[inline]
    fn read_bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.read_varint()? as usize;
        let end = self.pos.checked_add(len)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    #[inline]
    fn read_str(&mut self) -> Option<&'a str> {
        std::str::from_utf8(self.read_bytes()?).ok()
    }
    #[inline]
    fn skip(&mut self, wire: u8) -> Option<()> {
        match wire {
            0 => {
                self.read_varint()?;
            }
            1 => {
                self.pos = self.pos.checked_add(8)?;
                if self.pos > self.buf.len() {
                    return None;
                }
            }
            2 => {
                self.read_bytes()?;
            }
            5 => {
                self.pos = self.pos.checked_add(4)?;
                if self.pos > self.buf.len() {
                    return None;
                }
            }
            _ => return None,
        }
        Some(())
    }
}

pub struct Wrapper<'a> {
    pub channel: &'a str,
    pub body: &'a [u8],
}

/// Decode `PushDataV3ApiWrapper`: channel=field1, body=oneof field in 301..=399.
pub fn decode_wrapper(buf: &[u8]) -> Option<Wrapper<'_>> {
    let mut pb = Pb::new(buf);
    let mut channel = "";
    let mut body: &[u8] = &[];
    while !pb.at_end() {
        let (field, wire) = pb.read_tag()?;
        match (field, wire) {
            (1, 2) => channel = pb.read_str()?,
            (301..=399, 2) => body = pb.read_bytes()?,
            _ => pb.skip(wire)?,
        }
    }
    if channel.is_empty() {
        return None;
    }
    Some(Wrapper { channel, body })
}

pub struct Deal<'a> {
    pub price: &'a str,
    pub quantity: &'a str,
    /// 1 = taker buy (aggressor lifted ask → Bid), 2 = taker sell (→ Ask).
    pub trade_type: i32,
    pub time: i64,
}

/// `PublicAggreDealsV3Api`: deals=1 (repeated). item{price=1, quantity=2,
/// tradeType=int32 3, time=int64 4}.
pub fn decode_deals(body: &[u8]) -> Option<Vec<Deal<'_>>> {
    let mut pb = Pb::new(body);
    let mut deals = Vec::new();
    while !pb.at_end() {
        let (field, wire) = pb.read_tag()?;
        match (field, wire) {
            (1, 2) => {
                if let Some(d) = decode_deal(pb.read_bytes()?) {
                    deals.push(d);
                }
            }
            _ => pb.skip(wire)?,
        }
    }
    Some(deals)
}

fn decode_deal(body: &[u8]) -> Option<Deal<'_>> {
    let mut pb = Pb::new(body);
    let mut price = "";
    let mut quantity = "";
    let mut trade_type: i32 = 0;
    let mut time: i64 = 0;
    while !pb.at_end() {
        let (field, wire) = pb.read_tag()?;
        match (field, wire) {
            (1, 2) => price = pb.read_str()?,
            (2, 2) => quantity = pb.read_str()?,
            (3, 0) => trade_type = pb.read_varint()? as i32,
            (4, 0) => time = pb.read_varint()? as i64,
            _ => pb.skip(wire)?,
        }
    }
    if price.is_empty() {
        return None;
    }
    Some(Deal { price, quantity, trade_type, time })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let mut b = (v & 0x7F) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                break;
            }
        }
    }
    fn put_tag(out: &mut Vec<u8>, field: u32, wire: u8) {
        put_varint(out, ((field as u64) << 3) | wire as u64);
    }
    fn put_str(out: &mut Vec<u8>, field: u32, s: &str) {
        put_tag(out, field, 2);
        put_varint(out, s.len() as u64);
        out.extend_from_slice(s.as_bytes());
    }
    fn put_msg(out: &mut Vec<u8>, field: u32, msg: &[u8]) {
        put_tag(out, field, 2);
        put_varint(out, msg.len() as u64);
        out.extend_from_slice(msg);
    }
    fn put_vint_field(out: &mut Vec<u8>, field: u32, v: u64) {
        put_tag(out, field, 0);
        put_varint(out, v);
    }

    #[test]
    fn decodes_deals_wrapper() {
        let mut item = Vec::new();
        put_str(&mut item, 1, "100.5");
        put_str(&mut item, 2, "0.25");
        put_vint_field(&mut item, 3, 2); // sell
        put_vint_field(&mut item, 4, 1_700_000_000_123);
        let mut body = Vec::new();
        put_msg(&mut body, 1, &item);
        put_str(&mut body, 2, "spot@public.aggre.deals.v3.api.pb");

        let mut frame = Vec::new();
        put_str(&mut frame, 1, "spot@public.aggre.deals.v3.api.pb@100ms@BTCUSDT");
        put_msg(&mut frame, 301, &body);

        let w = decode_wrapper(&frame).expect("wrapper");
        assert!(w.channel.contains("deals"));
        assert_eq!(w.channel.split('@').next_back(), Some("BTCUSDT"));
        let deals = decode_deals(w.body).expect("deals");
        assert_eq!(deals.len(), 1);
        assert_eq!(deals[0].price, "100.5");
        assert_eq!(deals[0].quantity, "0.25");
        assert_eq!(deals[0].trade_type, 2);
        assert_eq!(deals[0].time, 1_700_000_000_123);
    }

    #[test]
    fn rejects_truncated() {
        assert!(decode_wrapper(&[0x0A, 0x0A, 0x01, 0x02]).is_none());
    }
}
