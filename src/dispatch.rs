use futures_util::StreamExt;
use log::trace;
use simd_json::Mutable;
use tokio::{sync::broadcast, time::interval};
use twilight_gateway::{
    shard::{Events, Stage},
    Event,
};

use std::{sync::Arc, time::Duration};

use crate::{
    deserializer::{EventTypeInfo, GatewayEventDeserializer, SequenceInfo},
    model::Ready,
    state::ShardStatus,
};

pub type BroadcastMessage = (String, Option<SequenceInfo>);

pub async fn dispatch_events(
    mut events: Events,
    shard_status: Arc<ShardStatus>,
    shard_id: u64,
    broadcast_tx: broadcast::Sender<BroadcastMessage>,
) {
    while let Some(event) = events.next().await {
        shard_status.guilds.update(&event);

        if let Event::ShardPayload(body) = event {
            let mut payload = unsafe { String::from_utf8_unchecked(body.bytes) };
            // The event is always valid
            let deserializer = GatewayEventDeserializer::from_json(&payload).unwrap();
            let (op, sequence, event_type) = deserializer.into_parts();

            if let Some(EventTypeInfo(event_name, _)) = event_type {
                metrics::increment_counter!("gateway_shard_events", "shard" => shard_id.to_string(), "event_type" => event_name.to_string());

                if event_name == "READY" {
                    // Use the raw JSON from READY to create a new blank READY
                    let mut ready: Ready = simd_json::from_str(&mut payload).unwrap();

                    // Clear the guilds
                    if let Some(guilds) = ready.d.get_mut("guilds") {
                        if let Some(arr) = guilds.as_array_mut() {
                            arr.clear();
                        }
                    }

                    // We don't care if it was already set
                    // since this data is timeless
                    let _res = shard_status.ready.set(ready.d);
                    shard_status.ready_set.notify_waiters();

                    continue;
                } else if event_name == "RESUMED" {
                    continue;
                }
            }

            // We only want to relay dispatchable events, not RESUMEs and not READY
            // because we fake a READY event
            if op.0 == 0 {
                trace!(
                    "[Shard {}] Sending payload to clients: {:?}",
                    shard_id,
                    payload
                );
                let _res = broadcast_tx.send((payload, sequence));
            }
        }
    }
}

pub async fn shard_latency(shard_status: Arc<ShardStatus>) {
    let mut interval = interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        if let Ok(info) = shard_status.shard.info() {
            // There is no way around this, sadly
            let connection_status = match info.stage() {
                Stage::Connected => 4.0,
                Stage::Disconnected => 0.0,
                Stage::Handshaking => 1.0,
                Stage::Identifying => 2.0,
                Stage::Resuming => 3.0,
                _ => f64::NAN,
            };

            let latency = info
                .latency()
                .recent()
                .get(0)
                .map_or(f64::NAN, Duration::as_secs_f64);

            metrics::histogram!("gateway_shard_latency", latency, "shard" => info.id().to_string());
            metrics::histogram!("gateway_shard_status", connection_status, "shard" => info.id().to_string());
        }
    }
}
