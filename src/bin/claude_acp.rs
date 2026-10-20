#[tokio::main]
async fn main() {
    if let Err(error) = soulsteads_messaging::adapters::claude::run().await {
        eprintln!("Claude ACP adapter failed: {error}");
        std::process::exit(1);
    }
}
