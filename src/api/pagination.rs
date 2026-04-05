use serde::{Deserialize, Serialize};

use crate::support::pagination::Page;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiPaginationMeta {
    pub limit: u32,
    pub offset: u64,
    pub total: u64,
    pub has_more: bool,
    pub next_offset: Option<u64>,
    pub next_cursor: Option<String>,
}

impl ApiPaginationMeta {
    pub fn from_page<T>(page: &Page<T>) -> Self {
        let has_more = page.has_more();
        Self {
            limit: page.request.limit,
            offset: page.request.offset,
            total: page.total,
            has_more,
            next_offset: has_more.then_some(page.request.next_offset()),
            next_cursor: None,
        }
    }
}
