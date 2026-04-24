use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use crate::{Result, WalError, FRAME_HEADER_LEN, MAX_FRAME_LEN};

/// One-shot forward reader. Yields each intact frame; on the first torn
/// or corrupted frame it stops and reports the offset, which the caller
/// can truncate the file to if starting a new writer on top.
pub struct WalReader {
    file: File,
    offset: u64,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path).await?;
        Ok(Self { file, offset: 0 })
    }

    pub async fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        match self.file.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(WalError::Io(e)),
        }

        let len = u32::from_le_bytes(header[..4].try_into().expect("slice size checked"));
        let expected_crc = u32::from_le_bytes(header[4..].try_into().expect("slice size checked"));

        if len > MAX_FRAME_LEN {
            return Err(WalError::FrameTooLarge {
                len: len as u64,
                max: MAX_FRAME_LEN as u64,
            });
        }

        let mut payload = vec![0u8; len as usize];
        match self.file.read_exact(&mut payload).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                let have =
                    self.file.stream_position().await? - self.offset - FRAME_HEADER_LEN as u64;
                return Err(WalError::Truncated {
                    offset: self.offset,
                    need: len as u64,
                    have,
                });
            }
            Err(e) => return Err(WalError::Io(e)),
        }

        let got_crc = crc32fast::hash(&payload);
        if got_crc != expected_crc {
            return Err(WalError::CrcMismatch {
                offset: self.offset,
                got: got_crc,
                expected: expected_crc,
            });
        }

        self.offset += FRAME_HEADER_LEN as u64 + len as u64;
        Ok(Some(payload))
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub async fn reset(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0)).await?;
        self.offset = 0;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::writer::WalWriter;

    #[tokio::test]
    async fn roundtrip_three_frames() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut w = WalWriter::open(&path).await.unwrap();
            w.append(b"alpha").await.unwrap();
            w.append(b"bravo-longer").await.unwrap();
            w.append(&[0u8; 1024]).await.unwrap();
            w.flush_sync().await.unwrap();
        }

        let mut r = WalReader::open(&path).await.unwrap();
        assert_eq!(
            r.next_frame().await.unwrap().as_deref(),
            Some(&b"alpha"[..])
        );
        assert_eq!(
            r.next_frame().await.unwrap().as_deref(),
            Some(&b"bravo-longer"[..])
        );
        let third = r.next_frame().await.unwrap().unwrap();
        assert_eq!(third.len(), 1024);
        assert!(third.iter().all(|&b| b == 0));
        assert!(r.next_frame().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn corrupted_payload_reports_crc_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.wal");

        {
            let mut w = WalWriter::open(&path).await.unwrap();
            w.append(b"original").await.unwrap();
            w.flush_sync().await.unwrap();
        }

        let mut bytes = tokio::fs::read(&path).await.unwrap();
        // Flip one byte inside the payload.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        tokio::fs::write(&path, &bytes).await.unwrap();

        let mut r = WalReader::open(&path).await.unwrap();
        let err = r.next_frame().await.unwrap_err();
        matches!(err, WalError::CrcMismatch { .. });
    }
}
