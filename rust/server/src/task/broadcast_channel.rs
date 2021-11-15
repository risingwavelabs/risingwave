use crate::task::channel::{BoxChanReceiver, BoxChanSender, ChanReceiver, ChanSender};
use risingwave_common::array::DataChunk;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::plan::exchange_info::BroadcastInfo;
use risingwave_pb::plan::*;
use risingwave_pb::{ToProst, ToProto};
use std::sync::mpsc;

/// `BroadcastSender` sends the same chunk to a number of `BroadcastReceiver`s.
pub struct BroadcastSender {
    senders: Vec<mpsc::Sender<DataChunk>>,
    broadcast_info: BroadcastInfo,
}

#[async_trait::async_trait]
impl ChanSender for BroadcastSender {
    async fn send(&mut self, chunk: DataChunk) -> Result<()> {
        self.senders.iter().try_for_each(|sender| {
            sender
                .send(chunk.clone())
                .to_rw_result_with("BroadcastSender::send")
        })
    }
}

/// One or more `BroadcastReceiver`s corresponds to a single `BroadcastReceiver`
pub struct BroadcastReceiver {
    receiver: mpsc::Receiver<DataChunk>,
}

#[async_trait::async_trait]
impl ChanReceiver for BroadcastReceiver {
    async fn recv(&mut self) -> Option<DataChunk> {
        match self.receiver.recv() {
            Err(_) => None, // Sender is dropped.
            Ok(chunk) => Some(chunk),
        }
    }
}

pub fn new_broadcast_channel(shuffle: &ExchangeInfo) -> (BoxChanSender, Vec<BoxChanReceiver>) {
    let shuffle_proto = shuffle.to_proto::<risingwave_proto::plan::ExchangeInfo>();
    let broadcast_info = shuffle_proto.get_broadcast_info();

    let output_count = broadcast_info.count as usize;
    let mut senders = Vec::with_capacity(output_count);
    let mut receivers = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let (s, r) = mpsc::channel();
        senders.push(s);
        receivers.push(r);
    }
    let channel_sender = Box::new(BroadcastSender {
        senders,
        broadcast_info: broadcast_info.clone().to_prost(),
    }) as BoxChanSender;
    let channel_receivers = receivers
        .into_iter()
        .map(|receiver| Box::new(BroadcastReceiver { receiver }) as BoxChanReceiver)
        .collect::<Vec<_>>();
    (channel_sender, channel_receivers)
}

#[cfg(test)]
mod tests {
    use crate::task::test_utils::{ResultChecker, TestRunner};
    use rand::Rng;
    use risingwave_pb::plan::ExchangeInfo;
    use risingwave_pb::plan::*;

    fn broadcast_plan(plan: &mut PlanFragment, num_sinks: u32) {
        let broadcast_info = exchange_info::BroadcastInfo { count: num_sinks };
        let distribution: exchange_info::Distribution =
            exchange_info::Distribution::BroadcastInfo(broadcast_info);

        plan.exchange_info = Some(ExchangeInfo {
            mode: exchange_info::DistributionMode::Broadcast as i32,
            distribution: Some(distribution),
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_broadcast() {
        async fn test_case(num_columns: usize, num_rows: usize, num_sinks: u32) {
            let mut rng = rand::thread_rng();
            let mut rows = vec![];
            for _row_idx in 0..num_rows {
                let mut row = vec![];
                for _col_idx in 0..num_columns {
                    row.push(rng.gen::<i32>());
                }
                rows.push(row);
            }
            let mut columns = vec![vec![]; num_columns];
            for (_row_idx, row) in rows.iter().enumerate() {
                for (col_idx, value) in row.iter().enumerate() {
                    columns[col_idx].push(*value);
                }
            }

            let mut runner = TestRunner::new();
            let mut table_builder = runner.prepare_table().create_table_int32s(num_columns);
            for row in &rows {
                table_builder = table_builder.insert_i32s(row);
            }
            table_builder.run().await;

            let mut builder = runner.prepare_scan().scan_all().await;
            broadcast_plan(builder.get_mut_plan(), num_sinks);
            let res = builder.run_and_collect_multiple_output().await;
            assert_eq!(num_sinks as usize, res.len());
            for (_, col) in res.into_iter().enumerate() {
                let mut res_checker = ResultChecker::new();
                for column in &columns {
                    res_checker.add_i32_column(false, column.as_slice());
                }
                res_checker.check_result(&col);
            }
        }

        test_case(1, 1, 3).await;
        test_case(2, 2, 5).await;
        test_case(10, 10, 5).await;
        test_case(100, 100, 7).await;
    }
}
