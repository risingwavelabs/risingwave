// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_meta_model::prelude::{Actor, Fragment};
use risingwave_meta_model::{actor, fragment};
use sea_orm::sea_query::{Expr, Func};
use sea_orm::{DatabaseConnection, EntityTrait, QuerySelect};

use crate::MetaResult;

pub type IdCategoryType = u8;

// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/95174) get completed.
#[expect(non_snake_case, non_upper_case_globals)]
pub mod IdCategory {
    use super::IdCategoryType;

    #[cfg(test)]
    pub const Test: IdCategoryType = 0;
    pub const Table: IdCategoryType = 1;
    pub const Fragment: IdCategoryType = 2;
    pub const Actor: IdCategoryType = 3;
}
pub struct IdGenerator<const TYPE: IdCategoryType>(AtomicU64);

impl<const TYPE: IdCategoryType> IdGenerator<TYPE> {
    pub async fn new(conn: &DatabaseConnection) -> MetaResult<Self> {
        let id: i32 = match TYPE {
            IdCategory::Table => {
                // Since we are using object pk to generate id for tables, here we just implement a dummy
                // id generator and refill it later when inserting the table.
                0
            }
            IdCategory::Fragment => Fragment::find()
                .select_only()
                .expr(Func::if_null(
                    Expr::col(fragment::Column::FragmentId).max().add(1),
                    1,
                ))
                .into_tuple()
                .one(conn)
                .await?
                .unwrap_or_default(),
            IdCategory::Actor => Actor::find()
                .select_only()
                .expr(Func::if_null(
                    Expr::col(actor::Column::ActorId).max().add(1),
                    1,
                ))
                .into_tuple()
                .one(conn)
                .await?
                .unwrap_or_default(),
            _ => unreachable!("IdGeneratorV2 only supports Table, Fragment, and Actor"),
        };

        Ok(Self(AtomicU64::new(id as u64)))
    }

    pub fn generate_interval(&self, interval: u64) -> u64 {
        self.0.fetch_add(interval, Ordering::Relaxed)
    }
}

pub type IdGeneratorManagerRef = Arc<IdGeneratorManager>;

/// `IdGeneratorManager` is a manager for three id generators: `tables`, `fragments`, and `actors`. Note that this is just a
/// workaround for the current implementation of `IdGenerator`. We should refactor it later.
pub struct IdGeneratorManager {
    pub tables: Arc<IdGenerator<{ IdCategory::Table }>>,
    pub fragments: Arc<IdGenerator<{ IdCategory::Fragment }>>,
    pub actors: Arc<IdGenerator<{ IdCategory::Actor }>>,
}

impl IdGeneratorManager {
    pub async fn new(conn: &DatabaseConnection) -> MetaResult<Self> {
        Ok(Self {
            tables: Arc::new(IdGenerator::new(conn).await?),
            fragments: Arc::new(IdGenerator::new(conn).await?),
            actors: Arc::new(IdGenerator::new(conn).await?),
        })
    }

    pub fn generate<const C: IdCategoryType>(&self) -> u64 {
        match C {
            IdCategory::Table => self.tables.generate_interval(1),
            IdCategory::Fragment => self.fragments.generate_interval(1),
            IdCategory::Actor => self.actors.generate_interval(1),
            _ => unreachable!("IdGeneratorV2 only supports Table, Fragment, and Actor"),
        }
    }

    pub fn generate_interval<const C: IdCategoryType>(&self, interval: u64) -> u64 {
        match C {
            IdCategory::Table => self.tables.generate_interval(interval),
            IdCategory::Fragment => self.fragments.generate_interval(interval),
            IdCategory::Actor => self.actors.generate_interval(interval),
            _ => unreachable!("IdGeneratorV2 only supports Table, Fragment, and Actor"),
        }
    }
}
