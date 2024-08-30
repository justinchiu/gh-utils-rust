#[tokio::main]
async fn main() -> octocrab::Result<()> {
    use std::time::Instant;
    let start = Instant::now();
    let mut x = 0_u32;
    for i in 1..2_u32.pow(20) {
        x += 1;
    }
    let end = Instant::now();
    let duration = (end-start).as_secs_f64();
    println!("{duration}");

    let diff = octocrab::instance()
        .pulls("sqlfluff", "sqlfluff")
        .get_diff(1625)
        .await?;

    println!("{diff}");

    Ok(())
}
