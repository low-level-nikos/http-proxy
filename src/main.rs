use clap::Parser;
use http_proxy::server;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Args::parse();
    server::http_proxy(format!("{}:{}", args.host, args.port)).await
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The host the proxy shall be bound to
    #[arg(long)]
    host: String,

    /// The port the proxy shall be bound to
    #[arg(long)]
    port: u16,
}
