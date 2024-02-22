// Copyright 2024 RisingWave Labs
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

use risingwave_meta_model_v2::prelude::Actor;
use risingwave_meta_model_v2::{actor, actor_dispatcher, FragmentId};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, JoinType, QueryFilter, QuerySelect, RelationTrait,
    TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::MetaResult;

impl CatalogController {
    async fn test<C>(&self, txn: &C, fragment_ids: Vec<FragmentId>) -> MetaResult<()>
    where
        C: ConnectionTrait,
    {
        // downstream fragment ids
        let downstream_fragments: Vec<FragmentId> = Actor::find()
            .select_only()
            .column(actor::Column::FragmentId)
            .distinct()
            .join(JoinType::InnerJoin, actor_dispatcher::Relation::Actor.def())
            .filter(actor::Column::FragmentId.is_in(fragment_ids))
            .into_tuple()
            .all(txn)
            .await?;

        // let inner = self.inner.write().await?;
        // let txn = inner.db.begin().await?;
        //

        todo!()
    }
}

/// -- NOSHUFFLE DEPENDENCIES
// WITH RECURSIVE ShuffleDeps AS (SELECT a.fragment_id, ap.dispatcher_type, ap.dispatcher_id
//                                FROM actor a
//                                         JOIN actor_dispatcher ap ON a.actor_id = ap.actor_id
//                                WHERE ap.dispatcher_type = 'NO_SHUFFLE'
//                                  AND ap.dispatcher_id IN (8, 7)
//
//                                UNION ALL
//
//                                SELECT a.fragment_id, ap.dispatcher_type, ap.dispatcher_id
//                                FROM actor a
//                                         JOIN actor_dispatcher ap ON a.actor_id = ap.actor_id
//                                         JOIN ShuffleDeps sd ON sd.fragment_id = ap.dispatcher_id
//                                WHERE ap.dispatcher_type = 'NO_SHUFFLE')
// SELECT DISTINCT fragment_id, dispatcher_type, dispatcher_id
// FROM ShuffleDeps;
//
//
// -- to downstream
// select distinct a.fragment_id, ap.dispatcher_type, ap.dispatcher_id  from actor a, actor_dispatcher ap where a.actor_id = ap.actor_id and a.fragment_id in (4,8) ;
//
//
// -- to upstream
// select distinct a.fragment_id, ap.dispatcher_type, ap.dispatcher_id  from actor a, actor_dispatcher ap where a.actor_id = ap.actor_id and ap.dispatcher_id in (4,8) ;