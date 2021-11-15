use crate::task::channel::{BoxChanReceiver, BoxChanSender, ChanReceiver, ChanSender};
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_pb::plan::exchange_info::hash_info::HashMethod;
use risingwave_pb::plan::exchange_info::HashInfo;
use risingwave_pb::plan::*;
use risingwave_pb::{ToProst, ToProto};
use std::option::Option;
use std::sync::mpsc;

pub struct HashShuffleSender {
    senders: Vec<mpsc::Sender<DataChunk>>,
    hash_info: exchange_info::HashInfo,
}

pub struct HashShuffleReceiver {
    receiver: mpsc::Receiver<DataChunk>,
}

fn generate_hash_values(chunk: &DataChunk, hash_info: &HashInfo) -> Result<Vec<usize>> {
    let output_count = hash_info.output_count as usize;

    let hasher_builder = match hash_info.get_hash_method() {
        HashMethod::Crc32 => CRC32FastBuilder {},
    };

    let hash_values = chunk
        .get_hash_values(
            &hash_info
                .keys
                .iter()
                .map(|key| *key as usize)
                .collect::<Vec<_>>(),
            hasher_builder,
        )
        .map_err(|e| InternalError(format!("get_hash_values:{}", e)))?
        .iter_mut()
        .map(|hash_value| *hash_value as usize % output_count)
        .collect::<Vec<_>>();
    Ok(hash_values)
}

fn generate_new_data_chunks(
    chunk: &DataChunk,
    hash_info: &exchange_info::HashInfo,
    hash_values: &[usize],
) -> Result<Vec<DataChunk>> {
    let output_count = hash_info.output_count as usize;
    let mut vis_maps = vec![vec![]; output_count];
    hash_values.iter().for_each(|hash| {
        for (sink_id, vis_map) in vis_maps.iter_mut().enumerate() {
            if *hash == sink_id {
                vis_map.push(true);
            } else {
                vis_map.push(false);
            }
        }
    });
    let mut res = Vec::with_capacity(output_count);
    for (sink_id, vis_map_vec) in vis_maps.into_iter().enumerate() {
        let vis_map = (vis_map_vec).try_into()?;
        let new_data_chunk = chunk.with_visibility(vis_map).compact()?;
        debug!(
            "send to sink:{}, cardinality:{}",
            sink_id,
            new_data_chunk.cardinality()
        );
        res.push(new_data_chunk);
    }
    Ok(res)
}

#[async_trait::async_trait]
impl ChanSender for HashShuffleSender {
    async fn send(&mut self, chunk: DataChunk) -> Result<()> {
        let hash_values = generate_hash_values(&chunk, &self.hash_info)?;
        let new_data_chunks = generate_new_data_chunks(&chunk, &self.hash_info, &hash_values)?;

        for (sink_id, new_data_chunk) in new_data_chunks.into_iter().enumerate() {
            debug!(
                "send to sink:{}, cardinality:{}",
                sink_id,
                new_data_chunk.cardinality()
            );
            self.senders[sink_id]
                .send(new_data_chunk)
                .to_rw_result_with("HashShuffleSender::send")?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ChanReceiver for HashShuffleReceiver {
    async fn recv(&mut self) -> Option<DataChunk> {
        match self.receiver.recv() {
            Err(_) => None, // Sender is dropped.
            Ok(chunk) => Some(chunk),
        }
    }
}

pub fn new_hash_shuffle_channel(shuffle: &ExchangeInfo) -> (BoxChanSender, Vec<BoxChanReceiver>) {
    let shuffle_proto = shuffle.to_proto::<risingwave_proto::plan::ExchangeInfo>();
    let hash_info = shuffle_proto.get_hash_info();

    let output_count = hash_info.output_count as usize;
    let mut senders = Vec::with_capacity(output_count);
    let mut receivers = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let (s, r) = mpsc::channel();
        senders.push(s);
        receivers.push(r);
    }
    let channel_sender = Box::new(HashShuffleSender {
        senders,
        hash_info: hash_info.to_prost(),
    }) as BoxChanSender;
    let channel_receivers = receivers
        .into_iter()
        .map(|receiver| Box::new(HashShuffleReceiver { receiver }) as BoxChanReceiver)
        .collect::<Vec<_>>();
    (channel_sender, channel_receivers)
}

#[cfg(test)]
mod tests {
    use crate::task::test_utils::{ResultChecker, TestRunner};
    use rand::Rng;
    use risingwave_common::util::hash_util::CRC32FastBuilder;
    use risingwave_pb::plan::exchange_info::hash_info::HashMethod;
    use risingwave_pb::plan::exchange_info::DistributionMode;
    use risingwave_pb::plan::exchange_info::HashInfo;
    use risingwave_pb::plan::*;
    use std::hash::BuildHasher;

    pub fn hash_shuffle_plan(plan: &mut PlanFragment, keys: Vec<u32>, num_sinks: u32) {
        let hash_info = HashInfo {
            output_count: num_sinks,
            hash_method: HashMethod::Crc32 as i32,
            keys,
        };

        let distribution: exchange_info::Distribution =
            exchange_info::Distribution::HashInfo(hash_info);

        let exchange_info = ExchangeInfo {
            mode: DistributionMode::Hash as i32,
            distribution: Some(distribution),
        };

        plan.exchange_info = Some(exchange_info);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hash_shuffle() {
        async fn test_case(num_columns: usize, num_rows: usize, num_sinks: u32, keys: Vec<u32>) {
            let mut rng = rand::thread_rng();
            let mut rows = vec![];
            for _row_idx in 0..num_rows {
                let mut row = vec![];
                for _col_idx in 0..num_columns {
                    row.push(rng.gen::<i32>());
                }
                rows.push(row);
            }

            let mut runner = TestRunner::new();
            let mut table_builder = runner.prepare_table().create_table_int32s(num_columns);
            for row in &rows {
                table_builder = table_builder.insert_i32s(row);
            }
            table_builder.run().await;

            let mut builder = runner.prepare_scan().scan_all().await;
            let hashes = rows
                .iter()
                .map(|row| {
                    let hash_builder = CRC32FastBuilder {};
                    let mut hasher = hash_builder.build_hasher();
                    keys.iter().for_each(|key| {
                        let bs = row[*key as usize].to_le_bytes();
                        hasher.update(&bs);
                    });
                    hasher.finalize() % num_sinks
                })
                .collect::<Vec<u32>>();
            let mut each_sink_output_columns = vec![vec![vec![]; num_columns]; num_sinks as usize];
            hashes.iter().zip(rows.iter()).for_each(|(hash, row)| {
                let output_columns = &mut each_sink_output_columns[*hash as usize];
                for (col_idx, num) in row.iter().enumerate() {
                    output_columns[col_idx].push(*num);
                }
            });
            hash_shuffle_plan(builder.get_mut_plan(), keys, num_sinks);
            let res = builder.run_and_collect_multiple_output().await;
            assert_eq!(num_sinks as usize, res.len());
            for (sink_id, col) in res.into_iter().enumerate() {
                let mut res_checker = ResultChecker::new();
                for column in each_sink_output_columns[sink_id].iter() {
                    res_checker.add_i32_column(false, column.as_slice());
                }
                res_checker.check_result(&col);
            }
        }

        test_case(1, 1, 3, vec![0]).await;
        test_case(2, 2, 5, vec![0]).await;
        test_case(10, 10, 5, vec![0, 3, 5]).await;
        test_case(100, 100, 7, vec![0, 2, 51, 98]).await;
    }
}
