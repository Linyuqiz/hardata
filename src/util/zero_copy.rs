use anyhow::Result;
use std::path::Path;
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IOBackend {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    IOUring,
    Tokio,
}

impl IOBackend {
    pub fn auto_detect() -> Self {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        {
            debug!("Using Linux optimized I/O (pread/pwrite with sendfile)");
            IOBackend::IOUring
        }

        #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
        {
            debug!("Using Tokio standard I/O");
            IOBackend::Tokio
        }
    }
}

pub struct FileOperator {
    backend: IOBackend,
}

impl FileOperator {
    pub fn new() -> Self {
        Self {
            backend: IOBackend::auto_detect(),
        }
    }

    pub async fn read_at(&self, path: &Path, offset: u64, length: usize) -> Result<Vec<u8>> {
        match self.backend {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            IOBackend::IOUring => self.read_at_uring(path, offset, length).await,
            IOBackend::Tokio => self.read_at_tokio(path, offset, length).await,
        }
    }

    pub async fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> Result<usize> {
        match self.backend {
            #[cfg(all(target_os = "linux", feature = "io-uring"))]
            IOBackend::IOUring => self.write_at_uring(path, offset, data).await,
            IOBackend::Tokio => self.write_at_tokio(path, offset, data).await,
        }
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    async fn read_at_uring(&self, path: &Path, offset: u64, length: usize) -> Result<Vec<u8>> {
        use std::fs::File;

        let path = path.to_path_buf();

        let offset_i64 = offset
            .try_into()
            .map_err(|_| anyhow::anyhow!("Offset too large: {}", offset))?;

        tokio::task::spawn_blocking(move || {
            let file = File::open(&path)?;
            let mut buffer = vec![0u8; length];

            let n = nix::sys::uio::pread(&file, &mut buffer, offset_i64)
                .map_err(|e| anyhow::anyhow!("pread failed: {}", e))?;

            buffer.truncate(n);
            Ok(buffer)
        })
        .await?
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    async fn write_at_uring(&self, path: &Path, offset: u64, data: &[u8]) -> Result<usize> {
        use std::fs::OpenOptions;

        let path = path.to_path_buf();
        let data = data.to_vec();

        let offset_i64 = offset
            .try_into()
            .map_err(|_| anyhow::anyhow!("Offset too large: {}", offset))?;

        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new().write(true).create(true).open(&path)?;

            let mut written = 0;
            let mut remaining = &data[..];
            let mut current_offset = offset_i64;

            while !remaining.is_empty() {
                let n = nix::sys::uio::pwrite(&file, remaining, current_offset)
                    .map_err(|e| anyhow::anyhow!("pwrite failed: {}", e))?;

                if n == 0 {
                    return Err(anyhow::anyhow!("pwrite returned 0, disk may be full"));
                }

                written += n;
                remaining = &remaining[n..];
                current_offset += n as i64;
            }

            Ok(written)
        })
        .await?
    }

    async fn read_at_tokio(&self, path: &Path, offset: u64, length: usize) -> Result<Vec<u8>> {
        use tokio::fs::File;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = File::open(path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    async fn write_at_tokio(&self, path: &Path, offset: u64, data: &[u8]) -> Result<usize> {
        use tokio::fs::OpenOptions;
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;

        file.seek(std::io::SeekFrom::Start(offset)).await?;
        file.write_all(data).await?;
        file.flush().await?;

        Ok(data.len())
    }
}

impl Default for FileOperator {
    fn default() -> Self {
        Self::new()
    }
}
