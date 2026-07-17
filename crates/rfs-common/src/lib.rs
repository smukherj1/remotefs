//! Domain primitives, diagnostics, and process logging shared across RemoteFS crates.

pub mod cas;
pub mod config;
pub mod control_protocol;
pub mod diagnostics;
pub mod digest;
pub mod error_context;
pub mod logging;
pub mod reapi;
pub mod state;
pub mod tree;
pub mod upload;

#[cfg(test)]
pub(crate) mod test_env {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    pub(crate) fn lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }
}
