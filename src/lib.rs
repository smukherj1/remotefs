pub mod cas;
pub mod cli;
pub mod config;
pub mod control;
pub mod digest;
pub mod error_context;
pub mod fs;
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
