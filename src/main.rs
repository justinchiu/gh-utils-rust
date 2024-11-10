mod github;

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

    for (repo, prs) in repo_prs {
        println!("Repository: {}", repo);
        println!("Number of PRs: {}", prs.len());
        for (pr, issues) in prs {
            println!("PR: {} - Issues: {:?}", pr.title.unwrap_or_default(), issues);
        }
    }
}
