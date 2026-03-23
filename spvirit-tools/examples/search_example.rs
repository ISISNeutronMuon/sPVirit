use std::time::Duration;

use spvirit_tools::{build_auto_broadcast_targets, search_pv};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pv_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MY:PV:NAME".into());

    let targets = build_auto_broadcast_targets();

    let addr = search_pv(&pv_name, 5076, Duration::from_secs(5), &targets, false).await?;
    println!("Found server at {addr}");
    Ok(())
}
