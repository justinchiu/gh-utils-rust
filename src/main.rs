mod hello;
mod github;

use github::get_pull_requests_with_issues;
use tokio;

#[tokio::main]
async fn main() {
    hello::hello();

    let repos = vec!["owner/repo1", "owner/repo2"];
    let repo_prs = get_pull_requests_with_issues(repos).await;

    for (repo, prs) in repo_prs {
        println!("Repository: {}", repo);
        for (pr, issues) in prs {
            println!("PR: {} - Issues: {:?}", pr.title.unwrap_or_default(), issues);
        }
    }
}
