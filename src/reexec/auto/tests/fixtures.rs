//! The mock connector and engine builder every topic below drives.

use super::*;

/// Records every call and serves a programmed value queue. Errors are
/// modeled by leaving the queue empty when `panic_on_empty` is false.
pub(super) struct MockConnector {
    pub(super) values: RefCell<alloc::vec::Vec<Value<Postgres>>>,
    pub(super) calls: RefCell<alloc::vec::Vec<(String, ScalarFamily)>>,
    pub(super) scalar_queries:
        RefCell<alloc::vec::Vec<crate::reexec::ReadQuery<'static, Postgres>>>,
    pub(super) page_queries: RefCell<alloc::vec::Vec<crate::reexec::ReadQuery<'static, Postgres>>>,
    pub(super) cursor_queries:
        RefCell<alloc::vec::Vec<crate::reexec::ReadQuery<'static, Postgres>>>,
    /// Pages a whole re-read serves, front first. Empty means the mock
    /// holds no cursors and `open_cursor` refuses.
    pub(super) cursor_pages: RefCell<alloc::vec::Vec<crate::reexec::RowPage<Postgres>>>,
    /// Pages `read_page` serves, popped from the back like `values`.
    /// Empty keeps the historic refusal, which the scalar tests rely on.
    pub(super) pages: RefCell<alloc::vec::Vec<crate::reexec::RowPage<Postgres>>>,
    /// Which `fetch_cursor` call fails, zero-based, if any. The async
    /// mock suspends at a fetch to model an abandoned read; the sync
    /// path has no suspension, so a read is left part way by a fetch
    /// that raises.
    pub(super) fail_fetch_at: RefCell<Option<usize>>,
    /// How many times `fetch_cursor` has been called.
    pub(super) fetches: RefCell<usize>,
    /// Interleaving log shared with the test's sink.
    pub(super) log: alloc::rc::Rc<RefCell<alloc::vec::Vec<&'static str>>>,
}

impl MockConnector {
    pub(super) fn new(values: alloc::vec::Vec<Value<Postgres>>) -> Self {
        Self {
            values: RefCell::new(values),
            calls: RefCell::new(alloc::vec::Vec::new()),
            scalar_queries: RefCell::new(alloc::vec::Vec::new()),
            page_queries: RefCell::new(alloc::vec::Vec::new()),
            cursor_queries: RefCell::new(alloc::vec::Vec::new()),
            cursor_pages: RefCell::new(alloc::vec::Vec::new()),
            pages: RefCell::new(alloc::vec::Vec::new()),
            fail_fetch_at: RefCell::new(None),
            fetches: RefCell::new(0),
            log: alloc::rc::Rc::new(RefCell::new(alloc::vec::Vec::new())),
        }
    }
    pub(super) fn call_count(&self) -> usize {
        self.calls.borrow().len()
    }
    pub(super) fn push_page(&self, page: crate::reexec::RowPage<Postgres>) {
        self.pages.borrow_mut().push(page);
    }
}

/// What the mock connector fails with, as one variant, because every failure
/// it can stage is "the test did not queue an answer for this read".
#[derive(Debug, PartialEq, thiserror::Error)]
pub(super) enum MockError {
    #[error("{0}")]
    Unstaged(&'static str),
}

impl Connector for MockConnector {
    type AuthContext = ();
    type Error = MockError;
    type Checkpoint = crate::NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &crate::reexec::ReadQuery<'_, Postgres>,
        column_kind: ScalarFamily,
        _auth: &(),
    ) -> Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error> {
        self.calls
            .borrow_mut()
            .push((String::from(query.sql()), column_kind));
        self.scalar_queries
            .borrow_mut()
            .push(query.clone().into_owned());
        let value = self
            .values
            .borrow_mut()
            .pop()
            .ok_or(MockError::Unstaged("queue empty"))?;
        Ok((value, None))
    }

    fn read_page(
        &self,
        query: &crate::reexec::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> Result<
        crate::reexec::connector::Snapshot<
            crate::reexec::connector::RowPage<Postgres>,
            Self::Checkpoint,
        >,
        Self::Error,
    > {
        self.page_queries
            .borrow_mut()
            .push(query.clone().into_owned());
        let popped = self.pages.borrow_mut().pop();
        let Some(page) = popped else {
            return Err(MockError::Unstaged(
                "read_page is not exercised by the scalar tests",
            ));
        };
        Ok(crate::reexec::connector::Snapshot {
            value: page,
            checkpoint: None,
        })
    }

    fn open_cursor(
        &self,
        query: &crate::reexec::ReadQuery<'_, Postgres>,
        _auth: &(),
    ) -> Result<crate::reexec::CursorId, crate::reexec::CursorError<Self::Error>> {
        self.cursor_queries
            .borrow_mut()
            .push(query.clone().into_owned());
        if self.cursor_pages.borrow().is_empty() {
            return Err(crate::reexec::CursorError::Unsupported);
        }
        self.log.borrow_mut().push("open");
        Ok(crate::reexec::CursorId(1))
    }

    fn fetch_cursor(
        &self,
        _cursor: crate::reexec::CursorId,
        _max_bytes: usize,
    ) -> Result<
        crate::reexec::connector::Snapshot<
            crate::reexec::connector::RowPage<Postgres>,
            Self::Checkpoint,
        >,
        crate::reexec::CursorError<Self::Error>,
    > {
        self.log.borrow_mut().push("fetch");
        let fetch = *self.fetches.borrow();
        *self.fetches.borrow_mut() = fetch + 1;
        if *self.fail_fetch_at.borrow() == Some(fetch) {
            return Err(crate::reexec::CursorError::Unsupported);
        }
        let page = self.cursor_pages.borrow_mut().remove(0);
        Ok(crate::reexec::connector::Snapshot {
            value: page,
            checkpoint: None,
        })
    }

    fn close_cursor(
        &self,
        _cursor: crate::reexec::CursorId,
    ) -> Result<(), crate::reexec::CursorError<Self::Error>> {
        self.log.borrow_mut().push("close");
        Ok(())
    }
}

pub(super) struct DisagreeingRegistrationRequest(
    pub(super) SubscriptionRequest<DefaultIds, Postgres>,
);

impl crate::RegistrationRequest<DefaultIds, Postgres> for DisagreeingRegistrationRequest {
    const DATABASE_READS_PER_CONSUMER: bool = false;

    fn into_request(self) -> SubscriptionRequest<DefaultIds, Postgres> {
        self.0
    }
}

pub(super) fn engine_with_values(
    values: alloc::vec::Vec<Value<Postgres>>,
) -> (
    AutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, SyncMode<MockConnector>>,
    TableId,
) {
    let database = catalog();
    let orders_id =
        crate::catalog_helpers::table_id::<crate::backend::Postgres, _>(&database, "orders")
            .expect("orders table exists");
    let inner = SubscriptionEngine::<TestEvent<Postgres>, DefaultIds, ParserDB>::new(
        database,
        PostgreSqlDialect {},
    );
    (
        AutoResolvingEngine::new(inner, SyncMode(MockConnector::new(values))),
        orders_id,
    )
}
