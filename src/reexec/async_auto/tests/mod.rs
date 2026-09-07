//! Tests for the asynchronous engine, one module per topic, mirroring the
//! synchronous suite's own topics: a mutation showed the async wrapper is held
//! to its behaviour only because this suite repeats it.

use super::super::connector::Snapshot;
use super::super::test_fixtures::{catalog, delete_event, insert_event, row, update_status_only};
use super::*;
use crate::backend::{Postgres, ScalarFamily};
use crate::testing::{block_on, TestEvent, YieldOnce};
use crate::{
    DefaultIds, NoCheckpoint, Registered, SubscriptionEngine, SubscriptionRequest, TableId, Tier,
};
use core::future::Future;
use core::pin::pin;
use core::task::Context;
use parking_lot::Mutex;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

mod aggregates;
mod binds;
mod debounce;
mod fixtures;
mod queue;
mod resolve;
mod rows;
mod throttle;
mod unregister;
