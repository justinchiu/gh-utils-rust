#[tokio::main]
async fn main() -> octocrab::Result<()> {
    let diff = octocrab::instance()
        .pulls("sqlfluff", "sqlfluff")
        .get_diff(1625)
        .await?;

    println!("{diff}");

    Ok(())
}
