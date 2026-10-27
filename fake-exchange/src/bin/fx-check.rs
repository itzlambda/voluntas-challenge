use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Compare collector SBE output against true exchange history")]
struct Args {
    sbe: PathBuf,
    history: PathBuf,
    #[arg(long)]
    allow_gaps: bool,
    #[arg(long, default_value_t = 5.0)]
    coverage_grace_secs: f64,
}
fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.coverage_grace_secs.is_finite() && args.coverage_grace_secs >= 0.0,
        "invalid coverage grace"
    );
    let summary = fake_exchange::check::check(&args.sbe, &args.history, args.coverage_grace_secs)?;
    println!("{}", serde_json::to_string(&summary)?);
    if !summary.passed(args.allow_gaps) {
        std::process::exit(1);
    }
    Ok(())
}
