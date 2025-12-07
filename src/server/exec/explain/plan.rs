#[derive(Debug, Clone)]
pub struct ExplainPlan {
    pub stmt: String,
    pub stages: Vec<ExplainStage>,
}

#[derive(Debug, Clone)]
pub struct ExplainStage {
    pub name: String,
    pub details: String,
}

impl ExplainPlan {
    pub fn new(stmt: impl Into<String>) -> Self {
        Self { stmt: stmt.into(), stages: Vec::new() }
    }
    pub fn with_stage(mut self, name: impl Into<String>, details: impl Into<String>) -> Self {
        self.stages.push(ExplainStage{ name: name.into(), details: details.into()});
        self
    }
}
