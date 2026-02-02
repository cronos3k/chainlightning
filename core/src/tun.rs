//! TUN device handling
//!
//! Platform-specific TUN interface for packet capture/injection.
//! Currently supports Linux only; provides stub implementations for other platforms.

use std::io::{Result as IoResult, Error as IoError, ErrorKind};

// Re-export the RawFd type for use by callers
#[cfg(target_os = "linux")]
pub use std::os::unix::io::RawFd;

#[cfg(not(target_os = "linux"))]
pub type RawFd = i32;

/// TUN device wrapper
pub struct TunDevice {
    #[cfg(target_os = "linux")]
    fd: RawFd,
    #[cfg(not(target_os = "linux"))]
    _fd: RawFd,
    name: String,
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use super::*;
    use std::os::unix::io::{AsRawFd, RawFd};

    /// TUN device flags
    const IFF_TUN: libc::c_short = 0x0001;
    const IFF_NO_PI: libc::c_short = 0x1000;
    const TUNSETIFF: libc::c_ulong = 0x400454ca;

    impl TunDevice {
        /// Create a new TUN device
        pub fn create(name: &str) -> IoResult<Self> {
            unsafe {
                // Open /dev/net/tun
                let fd = libc::open(
                    b"/dev/net/tun\0".as_ptr() as *const libc::c_char,
                    libc::O_RDWR | libc::O_NONBLOCK,
                );

                if fd < 0 {
                    return Err(IoError::last_os_error());
                }

                // Set up the interface
                let mut ifr: libc::ifreq = std::mem::zeroed();

                // Copy name (max 15 chars + null)
                let name_bytes = name.as_bytes();
                let copy_len = name_bytes.len().min(15);
                std::ptr::copy_nonoverlapping(
                    name_bytes.as_ptr(),
                    ifr.ifr_name.as_mut_ptr() as *mut u8,
                    copy_len,
                );

                // Set flags
                ifr.ifr_ifru.ifru_flags = IFF_TUN | IFF_NO_PI;

                // Configure device
                if libc::ioctl(fd, TUNSETIFF as libc::c_ulong, &ifr) < 0 {
                    libc::close(fd);
                    return Err(IoError::last_os_error());
                }

                // Get actual name
                let actual_name = std::ffi::CStr::from_ptr(ifr.ifr_name.as_ptr())
                    .to_string_lossy()
                    .into_owned();

                Ok(TunDevice {
                    fd,
                    name: actual_name,
                })
            }
        }

        /// Get the device name
        pub fn name(&self) -> &str {
            &self.name
        }

        /// Read a packet (non-blocking)
        pub fn read_packet(&self, buf: &mut [u8]) -> IoResult<usize> {
            let n = unsafe {
                libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
            };

            if n < 0 {
                let err = IoError::last_os_error();
                if err.kind() == ErrorKind::WouldBlock {
                    return Ok(0);
                }
                return Err(err);
            }

            Ok(n as usize)
        }

        /// Write a packet
        pub fn write_packet(&self, buf: &[u8]) -> IoResult<usize> {
            let n = unsafe {
                libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len())
            };

            if n < 0 {
                return Err(IoError::last_os_error());
            }

            Ok(n as usize)
        }

        /// Set blocking mode
        pub fn set_nonblocking(&self, nonblocking: bool) -> IoResult<()> {
            unsafe {
                let flags = libc::fcntl(self.fd, libc::F_GETFL);
                if flags < 0 {
                    return Err(IoError::last_os_error());
                }

                let new_flags = if nonblocking {
                    flags | libc::O_NONBLOCK
                } else {
                    flags & !libc::O_NONBLOCK
                };

                if libc::fcntl(self.fd, libc::F_SETFL, new_flags) < 0 {
                    return Err(IoError::last_os_error());
                }

                Ok(())
            }
        }

        /// Get raw file descriptor (for async registration)
        pub fn raw_fd(&self) -> RawFd {
            self.fd
        }
    }

    impl AsRawFd for TunDevice {
        fn as_raw_fd(&self) -> RawFd {
            self.fd
        }
    }

    impl Drop for TunDevice {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
impl TunDevice {
    /// Create a new TUN device (stub for non-Linux platforms)
    pub fn create(name: &str) -> IoResult<Self> {
        Ok(TunDevice {
            _fd: -1,
            name: name.to_string(),
        })
    }

    /// Get the device name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Read a packet (stub)
    pub fn read_packet(&self, _buf: &mut [u8]) -> IoResult<usize> {
        Err(IoError::new(ErrorKind::Unsupported, "TUN not supported on this platform"))
    }

    /// Write a packet (stub)
    pub fn write_packet(&self, _buf: &[u8]) -> IoResult<usize> {
        Err(IoError::new(ErrorKind::Unsupported, "TUN not supported on this platform"))
    }

    /// Set blocking mode (stub)
    pub fn set_nonblocking(&self, _nonblocking: bool) -> IoResult<()> {
        Ok(())
    }

    /// Get raw file descriptor (stub)
    pub fn raw_fd(&self) -> RawFd {
        -1
    }
}

/// Configure TUN device with IP addresses and MTU
#[cfg(target_os = "linux")]
pub fn configure_tun(name: &str, local_ip: &str, peer_ip: &str, mtu: u32) -> IoResult<()> {
    use std::process::Command;

    // ip addr add {local_ip} peer {peer_ip} dev {name}
    let status = Command::new("ip")
        .args(["addr", "add", local_ip, "peer", peer_ip, "dev", name])
        .status()?;

    if !status.success() {
        return Err(IoError::new(ErrorKind::Other, "Failed to add address"));
    }

    // ip link set dev {name} mtu {mtu}
    let status = Command::new("ip")
        .args(["link", "set", "dev", name, "mtu", &mtu.to_string()])
        .status()?;

    if !status.success() {
        return Err(IoError::new(ErrorKind::Other, "Failed to set MTU"));
    }

    // ip link set dev {name} up
    let status = Command::new("ip")
        .args(["link", "set", "dev", name, "up"])
        .status()?;

    if !status.success() {
        return Err(IoError::new(ErrorKind::Other, "Failed to bring up interface"));
    }

    // ip link set dev {name} txqueuelen 5000 — prevent kernel packet drops under load
    let status = Command::new("ip")
        .args(["link", "set", "dev", name, "txqueuelen", "5000"])
        .status()?;

    if !status.success() {
        tracing::warn!("Failed to set txqueuelen on {}", name);
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn configure_tun(_name: &str, _local_ip: &str, _peer_ip: &str, _mtu: u32) -> IoResult<()> {
    Err(IoError::new(ErrorKind::Unsupported, "TUN not supported on this platform"))
}

/// Delete TUN device
#[cfg(target_os = "linux")]
pub fn delete_tun(name: &str) -> IoResult<()> {
    use std::process::Command;

    let status = Command::new("ip")
        .args(["link", "delete", name])
        .status()?;

    if !status.success() {
        // May already be gone, not critical
        tracing::warn!("Failed to delete TUN device {}", name);
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn delete_tun(_name: &str) -> IoResult<()> {
    Ok(())
}
