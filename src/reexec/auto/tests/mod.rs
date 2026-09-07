//! Tests for the synchronous engine, one module per topic over one set
//! of fixtures.

use super::super::test_fixtures::{catalog, delete_event, insert_event, row, update_status_only};
use super::*;
use crate::backend::Postgres;
use crate::testing::TestEvent;
use crate::TableId;
use crate::{DefaultIds, SubscriptionEngine};
use core::cell::RefCell;
use sql_traits::structs::ParserDB;
use sqlparser::dialect::PostgreSqlDialect;

mod aggregates;
mod binds;
mod debounce;
mod fixtures;
mod queue;
mod resolve;
mod rows;
mod unregister;
