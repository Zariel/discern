#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRequest {
    pub limit: u32,
    pub offset: u64,
}

impl PageRequest {
    pub const DEFAULT_LIMIT: u32 = 50;
    pub const MAX_LIMIT: u32 = 200;

    pub fn new(limit: u32, offset: u64) -> Self {
        Self {
            limit: limit.min(Self::MAX_LIMIT),
            offset,
        }
    }

    pub fn next_offset(self) -> u64 {
        self.offset + u64::from(self.limit)
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            limit: Self::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub request: PageRequest,
    pub total: u64,
}

impl<T> Page<T> {
    pub fn has_more(&self) -> bool {
        self.request.next_offset() < self.total
    }
}

#[cfg(test)]
mod tests {
    use super::{Page, PageRequest};

    #[test]
    fn page_request_caps_limit_to_maximum() {
        let request = PageRequest::new(500, 10);

        assert_eq!(request.limit, PageRequest::MAX_LIMIT);
        assert_eq!(request.offset, 10);
    }

    #[test]
    fn page_reports_remaining_results() {
        let page = Page {
            items: vec![1, 2, 3],
            request: PageRequest::new(3, 0),
            total: 10,
        };

        assert!(page.has_more());
    }
}
