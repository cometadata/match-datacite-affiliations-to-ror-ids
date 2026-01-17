use anyhow::Result;
use clap::{Parser, Subcommand};
use datacite_ror::{extract, query, reconcile};

#[derive(Parser)]
#[command(name = "datacite-ror")]
#[command(about = "Extract affiliations from DataCite, query ROR, reconcile matches")]
#[command(version)]
#[command(propagate_version = true)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract unique affiliations and DOI/author relationships from DataCite files
    Extract(extract::ExtractArgs),
    /// Query affiliations against ROR API
    Query(query::QueryArgs),
    /// Reconcile ROR matches back to DOI/author records
    Reconcile(reconcile::ReconcileArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.verbose {
        std::env::set_var("RUST_LOG", "debug");
    }

    match cli.command {
        Commands::Extract(args) => extract::run(args),
        Commands::Query(args) => query::run(args),
        Commands::Reconcile(args) => reconcile::run(args),
    }
}
