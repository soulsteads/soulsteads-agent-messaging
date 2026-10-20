#[tokio::main]
async fn main() {
    if let Err(error) = soulsteads_messaging::adapters::codex::run().await {
        eprintln!("Codex ACP adapter failed: {error}");
        std::process::exit(1);
    }
}
