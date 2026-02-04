// Copyright 2026 RisingWave Labs
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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// Kafka lag per job, partition and fragment.
#[system_catalog(
    view,
    "rw_catalog.rw_kafka_job_lag",
    "with backfill_state as (
        select
            job_id,
            fragment_id,
            partition_id,
            backfill_progress,
            backfill_progress -> 'state' = to_jsonb('Finished'::text) as is_finished,
            coalesce(
                (backfill_progress -> 'state' ->> 'Backfilling')::bigint,
                (backfill_progress -> 'state' ->> 'SourceCachingUp')::bigint
            ) as backfill_offset
        from internal_source_backfill_progress()
    )
    select
        f.table_id as job_id,
        s.source_id,
        s.fragment_id,
        s.split_id as partition_id,
        case
            when bs.partition_id is not null and not coalesce(bs.is_finished, false) then 'BACKFILL'
            else 'LIVE'
        end as lag_phase,
        m.high_watermark,
        case
            when bs.partition_id is not null and not coalesce(bs.is_finished, false) then bs.backfill_offset
            else m.latest_offset
        end as consumer_offset,
        case
            when m.high_watermark is not null
                 and (
                    case
                        when bs.partition_id is not null and not coalesce(bs.is_finished, false) then bs.backfill_offset
                        else m.latest_offset
                    end
                 ) is not null
            then greatest(
                m.high_watermark - (
                    case
                        when bs.partition_id is not null and not coalesce(bs.is_finished, false) then bs.backfill_offset
                        else m.latest_offset
                    end
                ),
                0
            )
            else null
        end as lag
    from rw_actor_splits s
    join rw_fragments f on s.fragment_id = f.fragment_id
    join rw_kafka_source_metrics m
      on m.source_id = s.source_id
     and m.partition_id = s.split_id
    left join backfill_state bs
      on bs.job_id = f.table_id
     and bs.fragment_id = s.fragment_id
     and bs.partition_id = s.split_id"
)]
#[derive(Fields)]
struct RwKafkaJobLag {
    job_id: i32,
    source_id: i32,
    fragment_id: i32,
    partition_id: String,
    lag_phase: String,
    high_watermark: Option<i64>,
    consumer_offset: Option<i64>,
    lag: Option<i64>,
}
