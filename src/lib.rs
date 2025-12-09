pub mod server;
pub mod storage;
pub mod security;
pub mod scripts;
pub mod system;
pub mod system_catalog;
pub mod ident;
pub mod error;
pub mod lua_bc;
#[cfg(feature = "pgwire")]
pub mod pgwire_server;

// Test-only printing helper: expands to tprintln! during tests and is absent otherwise.
// Usage in tests: tprintln!("debug: {}", value);
#[cfg(any(test, debug_assertions))]
#[macro_export]
macro_rules! tprintln {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
    };
}

// In non-test builds, provide a no-op tprintln! so calls compile without effect.
#[cfg(not(any(test, debug_assertions)))]
#[macro_export]
macro_rules! tprintln {
    ($($arg:tt)*) => {
        if false { let _ = format!($($arg)*); }
    };
}
