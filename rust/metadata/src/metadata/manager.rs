use crate::metadata::watermark::Watermark;
use crate::metadata::{Config, Epoch, EpochGeneratorRef, MetaStoreRef, SINGLE_VERSION_EPOCH};
use protobuf::Message;
use risingwave_proto::metadata::EpochState;

pub struct MetaManager {
    pub meta_store_ref: MetaStoreRef,
    pub epoch_generator: EpochGeneratorRef,
    pub config: Config,

    // Frontend state fields.
    pub catalog_wm: Watermark,

    // TODO: more data could cached for single node deployment mode.
    // Backend state fields.
    current_epoch: Epoch,
    stable_epoch: Epoch,
}

impl MetaManager {
    pub async fn new(
        meta_store_ref: MetaStoreRef,
        epoch_generator: EpochGeneratorRef,
        config: Config,
    ) -> Self {
        let mut manager = MetaManager {
            meta_store_ref,
            epoch_generator,
            config,

            catalog_wm: Watermark::new(),
            current_epoch: Epoch::from(0),
            stable_epoch: Epoch::from(0),
        };
        let key = manager.config.get_epoch_state_key();
        let res = manager
            .meta_store_ref
            .get(key.as_bytes(), SINGLE_VERSION_EPOCH)
            .await;
        match res {
            Ok(value) => {
                let proto = EpochState::parse_from_bytes(&value).unwrap();
                manager.current_epoch = Epoch::from(proto.get_current_epoch());
                manager.stable_epoch = Epoch::from(proto.get_stable_epoch());
            }
            Err(err) => {
                if err.to_grpc_status().code() != tonic::Code::NotFound {
                    panic!("{}", err)
                }
            }
        }

        manager
    }
}
