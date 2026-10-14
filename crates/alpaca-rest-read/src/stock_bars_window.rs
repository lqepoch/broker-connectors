use std::{collections::HashSet, fmt};

use crate::{
    AlpacaRestError, AlpacaStockBarObservation, AlpacaStockBarsPage, AlpacaStockBarsRequest,
    StockBarsCursor,
};

/// Maximum provider pages read by one stock-bars window.
/// 单次股票 bars 窗口最多读取的 provider 页数。
pub const MAX_STOCK_BARS_PAGES_PER_WINDOW: u8 = 4;

/// A finite stock-bars read request with an optional continuation cursor.
/// 有界股票 bars 窗口；窗口达到页数上限时返回 continuation cursor。
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsWindowRequest {
    base: AlpacaStockBarsRequest,
    page_limit: u8,
}

impl AlpacaStockBarsWindowRequest {
    /// Create a finite read of one through four pages.
    ///
    /// `base.page_size()` remains the provider's per-page bar cap. The cursor, if present, must
    /// already be bound to the same exact stock-bars query.
    ///
    /// # Errors
    ///
    /// Returns [`AlpacaRestError::InvalidRequest`] when `page_limit` is outside 1 through 4.
    pub fn new(base: AlpacaStockBarsRequest, page_limit: u8) -> Result<Self, AlpacaRestError> {
        if page_limit == 0 || page_limit > MAX_STOCK_BARS_PAGES_PER_WINDOW {
            return Err(AlpacaRestError::InvalidRequest);
        }
        Ok(Self { base, page_limit })
    }

    /// Return the maximum pages requested for this finite window.
    #[must_use]
    pub const fn page_limit(&self) -> u8 {
        self.page_limit
    }

    pub(crate) fn into_parts(self) -> (AlpacaStockBarsRequest, u8) {
        (self.base, self.page_limit)
    }
}

impl fmt::Debug for AlpacaStockBarsWindowRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaStockBarsWindowRequest")
            .field("page_limit", &self.page_limit)
            .field("has_cursor", &self.base.cursor().is_some())
            .finish_non_exhaustive()
    }
}

/// Results from a finite set of stock-bars provider pages.
/// 结果仅覆盖本次有限窗口；存在后续游标时，返回的数据仍为部分结果。
#[derive(Clone, Eq, PartialEq)]
pub struct AlpacaStockBarsWindow {
    pages: Vec<AlpacaStockBarsPage>,
    next_cursor: Option<StockBarsCursor>,
}

impl AlpacaStockBarsWindow {
    pub(crate) const fn new(
        pages: Vec<AlpacaStockBarsPage>,
        next_cursor: Option<StockBarsCursor>,
    ) -> Self {
        Self { pages, next_cursor }
    }

    /// Borrow provider pages in the order they were read.
    #[must_use]
    pub fn pages(&self) -> &[AlpacaStockBarsPage] {
        &self.pages
    }

    /// Iterate bars in provider page order.
    pub fn bars(&self) -> impl Iterator<Item = &AlpacaStockBarObservation> {
        self.pages.iter().flat_map(|page| page.bars().iter())
    }

    /// Return the total number of bars in this window.
    #[must_use]
    pub fn bar_count(&self) -> usize {
        self.bars().count()
    }

    /// Return the number of provider pages read by this operation.
    #[must_use]
    pub fn pages_read(&self) -> usize {
        self.pages.len()
    }

    /// Return whether the provider supplied another page cursor.
    ///
    /// `false` means only that this page traversal received no next-page token; it is not a
    /// completeness, entitlement, or archive receipt.
    #[must_use]
    pub const fn has_more_pages(&self) -> bool {
        self.next_cursor.is_some()
    }

    /// Take the opaque cursor needed to continue the same query, if one remains.
    #[must_use]
    pub fn take_next_cursor(&mut self) -> Option<StockBarsCursor> {
        self.next_cursor.take()
    }
}

impl fmt::Debug for AlpacaStockBarsWindow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaStockBarsWindow")
            .field("pages_read", &self.pages.len())
            .field("bar_count", &self.bar_count())
            .field("has_more_pages", &self.next_cursor.is_some())
            .finish()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StockBarsPageProgress {
    Continue(Box<StockBarsCursor>),
    Stop,
}

pub(crate) struct StockBarsWindowBuilder {
    page_limit: u8,
    pages: Vec<AlpacaStockBarsPage>,
    seen_tokens: HashSet<String>,
    next_cursor: Option<StockBarsCursor>,
}

impl StockBarsWindowBuilder {
    pub(crate) fn new(page_limit: u8, initial_cursor: Option<&StockBarsCursor>) -> Self {
        let mut seen_tokens = HashSet::new();
        if let Some(cursor) = initial_cursor {
            seen_tokens.insert(cursor.provider_token().to_owned());
        }
        Self {
            page_limit,
            pages: Vec::with_capacity(usize::from(page_limit)),
            seen_tokens,
            next_cursor: None,
        }
    }

    pub(crate) fn push(
        &mut self,
        mut page: AlpacaStockBarsPage,
    ) -> Result<StockBarsPageProgress, AlpacaRestError> {
        if self.pages.len() >= usize::from(self.page_limit) {
            return Err(AlpacaRestError::ProtocolViolation);
        }

        let Some(cursor) = page.take_next_cursor() else {
            self.pages.push(page);
            self.next_cursor = None;
            return Ok(StockBarsPageProgress::Stop);
        };

        if !self.seen_tokens.insert(cursor.provider_token().to_owned()) {
            return Err(AlpacaRestError::ProtocolViolation);
        }

        self.pages.push(page);
        self.next_cursor = Some(cursor.clone());
        if self.pages.len() < usize::from(self.page_limit) {
            Ok(StockBarsPageProgress::Continue(Box::new(cursor)))
        } else {
            Ok(StockBarsPageProgress::Stop)
        }
    }

    pub(crate) fn finish(self) -> AlpacaStockBarsWindow {
        AlpacaStockBarsWindow::new(self.pages, self.next_cursor)
    }
}

#[cfg(test)]
#[path = "stock_bars_window_tests.rs"]
mod tests;
