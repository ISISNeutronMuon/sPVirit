use spvirit_client::PvaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pv = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MY:PV:NAME".into());

    let client = PvaClient::builder().build();
    let result = client.pvget(&pv).await?;
    println!("{}: {}", result.pv_name, result.value);
    Ok(())
}
