#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainFormat { Text, Json }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExplainOptions {
    pub format: ExplainFormat,
    pub verbose: bool,
}

impl Default for ExplainOptions {
    fn default() -> Self { Self { format: ExplainFormat::Text, verbose: false } }
}
