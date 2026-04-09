use spvirit_client::PvaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pv = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MY:PV:NAME".into());
    let value: f64 = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "42.0".into())
        .parse()?;

    let client = PvaClient::builder().build();
    client.pvput(&pv, value).await?;
    println!("OK");
    Ok(())
}
