//! Published overlay graph stubs.

use super::model::ResourceId;

#[derive(Debug, Clone)]
pub struct PublicationId(pub String);

#[derive(Debug, Clone)]
pub struct Jump {
    pub publication: PublicationId,
    pub virtual_path: String,
    pub target: ResourceId,
}
