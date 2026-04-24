pub mod reader;
pub mod writer;

pub use reader::WalReader;
pub use writer::WalWriter;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("frame too large: {len} bytes (max {max})")]
    FrameTooLarge { len: u64, max: u64 },

    #[error("crc mismatch at offset {offset}: got {got:08x}, expected {expected:08x}")]
    CrcMismatch {
        offset: u64,
        got: u32,
        expected: u32,
    },

    #[error("truncated frame at offset {offset}: need {need} bytes, have {have}")]
    Truncated { offset: u64, need: u64, have: u64 },
}

pub type Result<T> = std::result::Result<T, WalError>;

/// On-disk frame layout:
///   4 B   u32 LE  payload length
///   4 B   u32 LE  crc32 of payload
///   N B           payload bytes
pub const FRAME_HEADER_LEN: usize = 8;

/// Hard cap to refuse malformed headers during recovery. 64 MiB matches the
/// terminal's IPC frame ceiling, so it will never be legitimately exceeded.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;
