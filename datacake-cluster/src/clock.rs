use std::time::Duration;

use datacake_crdt::{get_unix_timestamp_ms, HLCTimestamp};
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct Clock {
    node_id: u32,
    tx: flume::Sender<Event>,
}

impl Clock {
    pub fn new(node_id: u32) -> Self {
        let ts = HLCTimestamp::new(get_unix_timestamp_ms(), 0, node_id);
        let (tx, rx) = flume::bounded(1000);

        tokio::spawn(run_clock(ts, rx));

        Self { node_id, tx }
    }

    pub async fn register_ts(&self, ts: HLCTimestamp) {
        if ts.node() == self.node_id {
            return;
        }

        self.tx
            .send_async(Event::Register(ts))
            .await
            .expect("Clock actor should never die");
    }

    pub async fn get_time(&self) -> HLCTimestamp {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send_async(Event::Get(tx))
            .await
            .expect("Clock actor should never die");

        rx.await.expect("Responder should not be dropped")
    }
}

pub enum Event {
    Get(oneshot::Sender<HLCTimestamp>),
    Register(HLCTimestamp),
}

async fn run_clock(mut clock: HLCTimestamp, reqs: flume::Receiver<Event>) {
    while let Ok(event) = reqs.recv_async().await {
        match event {
            Event::Get(tx) => {
                let ts = clock.send().expect("Clock counter should not overflow");

                if clock.counter() >= u16::MAX - 10 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }

                let _ = tx.send(ts);
            },
            Event::Register(remote_ts) => {
                let _ = clock.recv(&remote_ts);

                if clock.counter() >= u16::MAX - 10 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_clock() {
        let clock = Clock::new(0);

        let ts1 = clock.get_time().await;
        clock.register_ts(ts1).await;
        let ts2 = clock.get_time().await;
        assert!(ts1 < ts2);

        let ts1 = clock.get_time().await;
        let ts2 = clock.get_time().await;
        let ts3 = clock.get_time().await;
        assert!(ts1 < ts2);
        assert!(ts2 < ts3);

        let drift_ts = HLCTimestamp::new(get_unix_timestamp_ms() + 5_000, 0, 1);
        clock.register_ts(drift_ts).await;
        let ts = clock.get_time().await;
        assert!(
            drift_ts < ts,
            "New timestamp should be monotonic relative to drifted ts."
        );

        let old_ts = HLCTimestamp::new(get_unix_timestamp_ms() + 500_000, 0, 1);
        clock.register_ts(old_ts).await;
    }
}
