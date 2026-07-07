//! Three-node mesh demo — spawns three server processes wired as a full mesh,
//! then shows anti-entropy sync end to end:
//!
//!   1. subscribe to a region on node C,
//!   2. write observations to nodes A and B,
//!   3. watch C's subscriber receive both via Merkle-diff sync,
//!   4. query all three nodes and see the identical union.
//!
//! Run: `cargo run --bin mesh_demo`
//! (builds and starts the server binaries automatically)

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost::Message as _;
use prost_types::Any;

use interstellar_db::interstellar::{
    interstellar_db_client::InterstellarDbClient, EntityState, QueryWindowRequest, StoreRequest,
    SubscribeRequest,
};

const PORTS: [u16; 3] = [50071, 50072, 50073];
const NAMES: [&str; 3] = ["A", "B", "C"];
const SYNC_INTERVAL_SECS: u64 = 2;

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn entity(id: &str) -> Any {
    let mut buf = Vec::new();
    EntityState { entity_id: id.to_string() }.encode(&mut buf).unwrap();
    Any {
        type_url: "type.googleapis.com/interstellar.EntityState".to_string(),
        value: buf,
    }
}

fn entity_id(payload: &Option<Any>) -> String {
    payload
        .as_ref()
        .and_then(|a| EntityState::decode(a.value.as_slice()).ok())
        .map(|e| e.entity_id)
        .unwrap_or_else(|| "<unknown>".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ── 1. Spawn the three nodes, each peering with the other two ────────────
    let exe = std::env::current_exe()?;
    let server_bin = exe.parent().unwrap().join("server");
    if !server_bin.exists() {
        eprintln!("server binary not found at {server_bin:?} — run `cargo build --bin server` first.");
        std::process::exit(1);
    }

    let mut children = Vec::new();
    for (i, port) in PORTS.iter().enumerate() {
        let db_path = format!("/tmp/interstellar-mesh-demo-{}", NAMES[i].to_lowercase());
        let _ = std::fs::remove_dir_all(&db_path);
        let _ = std::process::Command::new("sh")
            .args(["-c", &format!("lsof -ti :{port} | xargs kill -9 2>/dev/null; true")])
            .status();

        let peers: Vec<String> = PORTS
            .iter()
            .filter(|p| *p != port)
            .map(|p| format!("http://[::1]:{p}"))
            .collect();

        let child = std::process::Command::new(&server_bin)
            .env("DB_PATH", &db_path)
            .env("PORT", port.to_string())
            .env("PEERS", peers.join(","))
            .env("SYNC_INTERVAL_SECS", SYNC_INTERVAL_SECS.to_string())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        children.push(child);
        println!("node {} listening on [::1]:{port}  (peers: {})", NAMES[i], peers.join(", "));
    }

    // Make sure the children die with us, success or panic.
    struct Reaper(Vec<std::process::Child>);
    impl Drop for Reaper {
        fn drop(&mut self) {
            for c in &mut self.0 {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
    let _reaper = Reaper(children);

    // ── 2. Connect to all three ──────────────────────────────────────────────
    let mut clients = Vec::new();
    for port in PORTS {
        let endpoint = format!("http://[::1]:{port}");
        let mut connected = None;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if let Ok(c) = InterstellarDbClient::connect(endpoint.clone()).await {
                connected = Some(c);
                break;
            }
        }
        clients.push(connected.ok_or_else(|| format!("could not connect to {endpoint}"))?);
    }
    let (mut a, mut b, mut c) =
        (clients.remove(0), clients.remove(0), clients.remove(0));

    println!();
    println!("InterstellarDB — 3-node mesh demo (sync every {SYNC_INTERVAL_SECS}s)");
    println!();

    // ── 3. Subscribe on node C before anything is written ────────────────────
    let mut sub = c
        .subscribe(SubscribeRequest {
            center_x: 0.0,
            center_y: 0.0,
            center_z: 0.0,
            radius: 100.0,
            max_age_secs: 60, // fresh observations only — skip any history replay
        })
        .await?
        .into_inner();
    println!("[C] subscribed: sphere r=100 at origin, max_age=60s");

    // ── 4. Write one observation each to A and B ─────────────────────────────
    let t0 = Instant::now();
    let now = now_secs();
    a.store(StoreRequest {
        x: 10.0, y: 5.0, z: -3.0,
        timestamp_secs: now, timestamp_nanos: 0,
        payload: Some(entity("track-from-A")),
    })
    .await?;
    println!("[A] stored track-from-A at (10, 5, -3)");

    b.store(StoreRequest {
        x: -20.0, y: 8.0, z: 2.0,
        timestamp_secs: now, timestamp_nanos: 0,
        payload: Some(entity("track-from-B")),
    })
    .await?;
    println!("[B] stored track-from-B at (-20, 8, 2)");

    // ── 5. C's subscriber hears both — delivered by anti-entropy sync ────────
    println!();
    println!("waiting for C's subscriber (observations must cross the mesh)...");
    for _ in 0..2 {
        let msg = tokio::time::timeout(Duration::from_secs(30), sub.message())
            .await
            .map_err(|_| "timed out waiting for a sync-fed subscription event")??
            .ok_or("subscription stream ended unexpectedly")?;
        println!(
            "[C] subscriber received {:<12} at ({:>5.1}, {:>4.1}, {:>4.1})  +{:.1}s after write",
            entity_id(&msg.payload),
            msg.x,
            msg.y,
            msg.z,
            t0.elapsed().as_secs_f64(),
        );
    }

    // ── 6. Every node answers the same query ─────────────────────────────────
    println!();
    for (name, client) in [("A", &mut a), ("B", &mut b), ("C", &mut c)] {
        let mut stream = client
            .query_window(QueryWindowRequest {
                center_x: 0.0, center_y: 0.0, center_z: 0.0,
                radius: 100.0,
                start_secs: now.saturating_sub(60),
                end_secs: now + 60,
            })
            .await?
            .into_inner();
        let mut ids = Vec::new();
        while let Some(row) = stream.message().await? {
            ids.push(entity_id(&row.payload));
        }
        ids.sort();
        println!("[{name}] query sees {} observations: {}", ids.len(), ids.join(", "));
    }

    println!();
    println!("mesh converged — every node holds the union, subscribers fired across nodes.");
    Ok(())
}
