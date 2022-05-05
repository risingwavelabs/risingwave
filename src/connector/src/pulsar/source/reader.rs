// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use mpsc::UnboundedSender;
use pulsar::{Consumer, ConsumerOptions, DeserializeMessage, Executor, Pulsar, TokioExecutor};
use pulsar::consumer::InitialPosition;
use pulsar::consumer::Message;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::base::{SourceMessage, SplitReader};
use crate::pulsar::split::{PulsarSplit};
use crate::pulsar::{
    PULSAR_CONFIG_ADMIN_URL_KEY, PULSAR_CONFIG_SERVICE_URL_KEY, PULSAR_CONFIG_TOPIC_KEY,
};
use crate::{AnyhowProperties, ConnectorStateV2, Properties};

struct PulsarSingleSplitReader {
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
}

impl PulsarSingleSplitReader
{
    async fn new(
        properties: AnyhowProperties,
        split: PulsarSplit,
    ) -> anyhow::Result<Self> {
        let service_url = properties.get(PULSAR_CONFIG_SERVICE_URL_KEY)?;
        let topic = properties.get(PULSAR_CONFIG_TOPIC_KEY)?;
        let mut builder = Pulsar::builder(service_url, TokioExecutor);
        let pulsar: Pulsar<_> = builder.build().await.map_err(|e| anyhow!(e))?;
        let mut consumer: Consumer<Vec<u8>, _> = pulsar.consumer()
            .with_topic(topic)
            .with_subscription("test-name-1-y")
            .with_options(ConsumerOptions::default().with_initial_position(InitialPosition::Earliest)).build().await.map_err(|e| anyhow!(e))?;
        // //
        // // match split.start_offset {
        // //     PulsarOffset::MessageID(msg_id) => {}
        // //     PulsarOffset::Timestamp(_) => {}
        // //     PulsarOffset::None => {}
        // // };
        //
        // // consumer.seek(None, Some(MessageIdData {
        // //     ledger_id: 19,
        // //     entry_id: 3,
        // //     partition: Some(1),
        // //     batch_index: Some(0),
        // //     ack_set: vec![],
        // //     batch_size: None,
        // // }), None, pulsar).await.unwrap();
        //
        // let x = consumer.next().await;
        Ok(Self {
            pulsar,
            consumer,
        })
    }

    async fn run(&mut self, stop: oneshot::Receiver<()>, output: mpsc::UnboundedSender<Message<Vec<u8>>>) -> Result<()> {
        loop  {
            let x: Option<Message<Vec<u8>>> = self.consumer.try_next().await?;

            let msg = match x {
                None => return Ok(()),
                Some(msg) => msg,
            };

            output.send(msg).map_err(|e| anyhow!("{}", e.to_string()))?;
        }

        // let stream = self.consumer.into_stream();
        // let x: Message<Vec<u8>> = self.consumer.borrow_mut().ready_chunks(PULSAR_MAX_FETCH_MESSAGES as usize).next().await?;
        //
        //
        //
        // println!("msg {:?}", x);

        // loop {
        //     tokio::select! {
        //     msg = stream => {
        //         todo!()
        //     }
        //
        //     _ = stop => {
        //         todo!()
        //     }
        //     }
        // }
        //
        todo!()
    }
}

pub struct PulsarSplitReader {
    // pulsar: Pulsar<TokioExecutor>,
    // consumer: Consumer<Vec<u8>, TokioExecutor>,
    // split: PulsarSplit,


    //    ch: tokio::sync::mpsc::UnboundedReceiver<Message<Vec<u8>>>,

    stop_chs: Vec<oneshot::Sender<()>>,
    ch: UnboundedReceiverStream<Message<Vec<u8>>>,
}

const PULSAR_MAX_FETCH_MESSAGES: u32 = 1024;

#[async_trait]
impl SplitReader for PulsarSplitReader {
    async fn next(&mut self) -> anyhow::Result<Option<Vec<SourceMessage>>> {
        // let x = UnboundedReceiverStream < Message < Vec < u8 >> >;

        let mut stream = self.ch.borrow_mut().ready_chunks(PULSAR_MAX_FETCH_MESSAGES as usize);
        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        // let mut stream = self
        //     .consumer
        //     .borrow_mut()
        //     .ready_chunks(PULSAR_MAX_FETCH_MESSAGES as usize);
        //

        let mut ret = Vec::with_capacity(chunk.len());

        for msg in chunk {

            //let msg = msg.map_err(|e| anyhow!(e))?;
//            let entry_id = msg.message_id.id.entry_id;

            // let should_stop = match self.split.stop_offset {
            //     PulsarOffset::MessageID(id) => entry_id >= id,
            //     PulsarOffset::Timestamp(timestamp) => {
            //         msg.payload.metadata.event_time() >= timestamp
            //     }
            //     PulsarOffset::None => false,
            // };

            // if should_stop {
            //     self.consumer
            //         .borrow_mut()
            //         .unsubscribe()
            //         .await
            //         .map_err(|e| anyhow!(e))?;
            //     break;
            //}

            ret.push(SourceMessage::from(msg));
        }

        Ok(Some(ret))
    }

    async fn new(_props: Properties, _state: ConnectorStateV2) -> Result<Self>
        where
            Self: Sized,
    {

        let (sender, receiver) = mpsc::unbounded_channel();


        let receiver = UnboundedReceiverStream::from(receiver);


        Ok(Self{
            stop_chs: vec![],
            ch: receiver,
        })
    }
}
