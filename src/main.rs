mod github;

use std::time::Instant;
use octocrab::Octocrab;
use github::get_pull_requests_with_issues;

#[tokio::main]
async fn main() {
    let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN not set");
    let octocrab = Octocrab::builder()
        .personal_token(token)
        .build()
        .unwrap();

    let repos = vec!["msiemens/tinydb"];
    let repo_prs = get_pull_requests_with_issues(&octocrab, repos).await;

    if let Some((repo, prs)) = repo_prs.iter().next() {
        println!("Repository: {}", repo);
        if let Some((pr, issues)) = prs.first() {
            println!("First PR Title: {}", pr.title.as_deref().unwrap_or("No title"));
            println!("First PR Body: {}", pr.body.as_deref().unwrap_or("No body"));
            println!("Issues: {:?}", issues);
        }
    }
}
