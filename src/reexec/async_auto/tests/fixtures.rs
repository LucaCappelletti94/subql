//! The mock async connector and engine builder every topic below drives.

use super::*;

/// `parking_lot::Mutex`-backed mock so the futures are `Send`.
pub(super) struct MockAsyncConnector {
    pub(super) values: Mutex<Vec<Value<Postgres>>>,
    pub(super) call_count: Mutex<usize>,
    pub(super) scalar_queries: Mutex<Vec<crate::reexec::ReadQuery<'static, Postgres>>>,
    pub(super) page_queries: Mutex<Vec<crate::reexec::ReadQuery<'static, Postgres>>>,
    pub(super) cursor_queries: Mutex<Vec<crate::reexec::ReadQuery<'static, Postgres>>>,
    /// When set, the next scalar read suspends once before answering, so
    /// a test can drop a resolve future mid-read.
    pub(super) pend_next_read: Mutex<bool>,
    /// Pages a whole re-read serves, front first. Empty means the mock
    /// holds no cursors and `open_cursor` refuses.
    pub(super) cursor_pages: Mutex<Vec<crate::reexec::RowPage<Postgres>>>,
    /// Pages `read_page` serves, popped from the back like `values`.
    /// Empty keeps the historic refusal, which the scalar tests rely on.
    pub(super) pages: Mutex<Vec<crate::reexec::RowPage<Postgres>>>,
    /// Fetch index that suspends once before serving, so a test can drop
    /// a resolve future between pages.
    pub(super) pend_fetch_at: Mutex<Option<usize>>,
    /// Fetches served so far.
    pub(super) fetch_count: Mutex<usize>,
    /// Interleaving log shared with the test's sink.
    pub(super) log: Arc<Mutex<Vec<&'static str>>>,
}

impl MockAsyncConnector {
    pub(super) fn new(values: Vec<Value<Postgres>>) -> Self {
        Self {
            values: Mutex::new(values),
            call_count: Mutex::new(0),
            scalar_queries: Mutex::new(Vec::new()),
            page_queries: Mutex::new(Vec::new()),
            cursor_queries: Mutex::new(Vec::new()),
            pend_next_read: Mutex::new(false),
            cursor_pages: Mutex::new(Vec::new()),
            pages: Mutex::new(Vec::new()),
            pend_fetch_at: Mutex::new(None),
            fetch_count: Mutex::new(0),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(super) fn call_count(&self) -> usize {
        *self.call_count.lock()
    }
    pub(super) fn push_page(&self, page: crate::reexec::RowPage<Postgres>) {
        self.pages.lock().push(page);
    }
}

/// What the mock connector fails with, as one variant, mirroring the
/// synchronous suite's own mock.
#[derive(Debug, thiserror::Error)]
pub(super) enum MockError {
    #[error("{0}")]
    Unstaged(&'static str),
}

// The `+ Send` bound on the returned futures is the whole point of
// the trait shape. `async fn in trait` cannot express it directly.
#[allow(clippy::manual_async_fn)]
impl AsyncConnector for MockAsyncConnector {
    type AuthContext = ();
    type Error = MockError;
    type Checkpoint = NoCheckpoint;
    type Backend = Postgres;

    fn execute_scalar(
        &self,
        query: &crate::reexec::ReadQuery<'_, Postgres>,
        _kind: ScalarFamily,
        _auth: &(),
    ) -> impl Future<Output = Result<(Value<Postgres>, Option<Self::Checkpoint>), Self::Error>> + Send
    {
        async move {
            if core::mem::take(&mut *self.pend_next_read.lock()) {
                YieldOnce(false).await;
            }
            *self.call_count.lock() += 1;
            self.scalar_queries.lock().push(query.clone().into_owned());
            let value = self
                .values
                .lock()
                .pop()
                .ok_or(MockError::Unstaged("queue empty"))?;
            Ok((value, None))
        }
    }

    fn read_page(
        &self,
        query: &crate::reexec::ReadQuery<'_, Postgres>,
        _max_bytes: usize,
        _auth: &(),
    ) -> impl Future<
        Output = Result<Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>, Self::Error>,
    > + Send {
        async move {
            self.page_queries.lock().push(query.clone().into_owned());
            let popped = self.pages.lock().pop();
            let Some(page) = popped else {
                return Err(MockError::Unstaged(
                    "read_page is not exercised by the scalar tests",
                ));
            };
            Ok(Snapshot {
                value: page,
                checkpoint: None,
            })
        }
    }

    fn open_cursor(
        &self,
        query: &crate::reexec::ReadQuery<'_, Postgres>,
        _auth: &(),
    ) -> impl Future<Output = Result<crate::reexec::CursorId, crate::reexec::CursorError<Self::Error>>>
           + Send {
        self.cursor_queries.lock().push(query.clone().into_owned());
        if self.cursor_pages.lock().is_empty() {
            return core::future::ready(Err(crate::reexec::CursorError::Unsupported));
        }
        self.log.lock().push("open");
        core::future::ready(Ok(crate::reexec::CursorId(1)))
    }

    fn fetch_cursor(
        &self,
        _cursor: crate::reexec::CursorId,
        _max_bytes: usize,
    ) -> impl Future<
        Output = Result<
            Snapshot<crate::reexec::RowPage<Postgres>, Self::Checkpoint>,
            crate::reexec::CursorError<Self::Error>,
        >,
    > + Send {
        async move {
            let index = {
                let mut count = self.fetch_count.lock();
                let index = *count;
                *count += 1;
                index
            };
            if *self.pend_fetch_at.lock() == Some(index) {
                YieldOnce(false).await;
            }
            self.log.lock().push("fetch");
            let page = self.cursor_pages.lock().remove(0);
            Ok(Snapshot {
                value: page,
                checkpoint: None,
            })
        }
    }

    fn close_cursor(
        &self,
        _cursor: crate::reexec::CursorId,
    ) -> impl Future<Output = Result<(), crate::reexec::CursorError<Self::Error>>> + Send {
        self.log.lock().push("close");
        core::future::ready(Ok(()))
    }
}

pub(super) fn engine_with_values(
    values: Vec<Value<Postgres>>,
) -> (
    AutoResolvingEngine<TestEvent<Postgres>, DefaultIds, ParserDB, AsyncMode<MockAsyncConnector>>,
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
        AutoResolvingEngine::new(inner, AsyncMode::new(MockAsyncConnector::new(values))),
        orders_id,
    )
}
