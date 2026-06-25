use std::io;
use std::os::fd::{AsRawFd, RawFd};

use tokio::io::unix::AsyncFd;

#[derive(Debug)]
pub struct EventFd {
    fd: RawFd,
}

impl EventFd {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { fd })
    }

    pub fn notify(&self) -> io::Result<()> {
        let value: u64 = 1;
        let rc = unsafe {
            libc::write(
                self.fd,
                (&value as *const u64).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if rc < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    pub fn wait(&self) -> io::Result<u64> {
        let mut value = 0u64;
        let rc = unsafe {
            libc::read(
                self.fd,
                (&mut value as *mut u64).cast::<libc::c_void>(),
                std::mem::size_of::<u64>(),
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(value)
    }

    pub fn into_async(self) -> io::Result<AsyncEventFd> {
        Ok(AsyncEventFd {
            inner: AsyncFd::new(self)?,
        })
    }
}

impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Drop for EventFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

#[derive(Debug)]
pub struct AsyncEventFd {
    inner: AsyncFd<EventFd>,
}

impl AsyncEventFd {
    pub async fn wait(&self) -> io::Result<u64> {
        loop {
            let mut guard = self.inner.readable().await?;
            match guard.try_io(|fd| fd.get_ref().wait()) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    pub fn notify(&self) -> io::Result<()> {
        self.inner.get_ref().notify()
    }
}
