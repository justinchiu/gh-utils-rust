mod github;

use github::get_pull_requests_with_issues;

#[tokio::main]
async fn main() {
    let repos = vec!["msiemens/tinydb"];
    let repo_prs = get_pull_requests_with_issues(repos).await;

    for (repo, prs) in repo_prs {
        println!("Repository: {}", repo);
        for (pr, issues) in prs {
            println!("PR: {} - Issues: {:?}", pr.title.unwrap_or_default(), issues);
        }
    }
}
