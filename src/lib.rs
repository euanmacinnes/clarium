pub mod server;
pub mod storage;
pub mod query;
pub mod security;
pub mod scripts;
pub mod system;
pub mod ident;
pub mod error;
#[cfg(feature = "pgwire")]
pub mod pgwire_server;

// Test-only printing helper: expands to tprintln! during tests and is absent otherwise.
// Usage in tests: tprintln!("debug: {}", value);
#[cfg(test)]
#[macro_export]
macro_rules! tprintln {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
    };
}

// In non-test builds, provide a no-op tprintln! so calls compile without effect.
#[cfg(not(test))]
#[macro_export]
macro_rules! tprintln {
    ($($arg:tt)*) => {
        if false { let _ = format!($($arg)*); }
    };
}
