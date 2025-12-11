use super::Principal;

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub principal: Option<Principal>,
    pub request_id: Option<String>,
    pub database: Option<String>,
    pub filestore: Option<String>,
}

impl Default for RequestContext {
    fn default() -> Self {
        Self { principal: None, request_id: None, database: None, filestore: None }
    }
}
