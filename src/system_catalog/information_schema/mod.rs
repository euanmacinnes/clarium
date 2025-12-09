pub mod schemata;
pub mod tables;
pub mod columns;
pub mod views;

pub fn register_defaults() {
    schemata::register();
    tables::register();
    columns::register();
    views::register();
}
