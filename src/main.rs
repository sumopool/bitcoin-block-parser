fn main() -> anyhow::Result<()> {
    #[cfg(feature = "examples")]
    bitcoin_block_parser::examples::run()?;
    Ok(())
}
