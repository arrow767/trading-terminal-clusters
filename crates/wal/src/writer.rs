use std::path::{Path, PathBuf};

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::{Result, WalError, FRAME_HEADER_LEN, MAX_FRAME_LEN};

/// Append-only writer that makes every `flush_sync` call durable before
/// returning. Crash recovery model: the ClickHouse sink never acknowledges
/// a cluster frame to upstream until the frame has cleared `flush_sync`
/// here, so the WAL file contains at least everything that was ever
/// acked. On restart, `WalReader` replays any tail that didn't make it
/// into ClickHouse.
pub struct WalWriter {
    file: File,
    path: PathBuf,
    bytes_written: u64,
}

impl WalWriter {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        let bytes_written = file.metadata().await?.len();
        Ok(Self {
            file,
            path,
            bytes_written,
        })
    }

    pub async fn append(&mut self, payload: &[u8]) -> Result<()> {
        let len = u32::try_from(payload.len()).map_err(|_| WalError::FrameTooLarge {
            len: payload.len() as u64,
            max: MAX_FRAME_LEN as u64,
        })?;
        if len > MAX_FRAME_LEN {
            return Err(WalError::FrameTooLarge {
                len: len as u64,
                max: MAX_FRAME_LEN as u64,
            });
        }

        let crc = crc32fast::hash(payload);
        let mut header = [0u8; FRAME_HEADER_LEN];
        header[..4].copy_from_slice(&len.to_le_bytes());
        header[4..].copy_from_slice(&crc.to_le_bytes());

        self.file.write_all(&header).await?;
        self.file.write_all(payload).await?;
        self.bytes_written += (FRAME_HEADER_LEN as u64) + (len as u64);
        Ok(())
    }

    /// Durable flush — returns only after the OS has confirmed the data
    /// is on stable storage. Use sparingly (batch appends between calls).
    pub async fn flush_sync(&mut self) -> Result<()> {
        self.file.flush().await?;
        self.file.sync_data().await?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}
