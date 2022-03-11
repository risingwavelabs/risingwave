use itertools::Itertools;
use tokio::sync::mpsc::unbounded_channel;

use super::*;

#[tokio::test]
async fn test_managed_barrier_collection() -> Result<()> {
    let mut manager = LocalBarrierManager::new();
    assert!(!manager.is_local_mode());

    let register_sender = |actor_id: u32| {
        let (barrier_tx, barrier_rx) = unbounded_channel();
        manager.register_sender(actor_id, barrier_tx);
        (actor_id, barrier_rx)
    };

    // Register actors
    let actor_ids = vec![233, 234, 235];
    let count = actor_ids.len();
    let mut rxs = actor_ids
        .clone()
        .into_iter()
        .map(register_sender)
        .collect_vec();

    // Send a barrier to all actors
    let epoch = 114514;
    let barrier = Barrier::new_test_barrier(epoch);
    let mut collect_rx = manager
        .send_barrier(&barrier, actor_ids.clone(), actor_ids)
        .unwrap()
        .unwrap();

    // Collect barriers from actors
    let collected_barriers = rxs
        .iter_mut()
        .map(|(actor_id, rx)| {
            let msg = rx.try_recv().unwrap();
            let barrier = match msg {
                Message::Barrier(b) => {
                    assert_eq!(b.epoch.curr, epoch);
                    b
                }
                _ => unreachable!(),
            };
            (*actor_id, barrier)
        })
        .collect_vec();

    // Report to local barrier manager
    for (i, (actor_id, barrier)) in collected_barriers.into_iter().enumerate() {
        manager.collect(actor_id, &barrier).unwrap();
        let notified = collect_rx.try_recv().is_ok();
        assert_eq!(notified, i == count - 1);
    }

    Ok(())
}
