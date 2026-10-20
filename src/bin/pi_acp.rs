#[tokio::main]
async fn main() {
    if let Err(error) = soulsteads_messaging::adapters::pi::run().await {
        eprintln!("Pi ACP adapter failed: {error}");
        std::process::exit(1);
    }
}
