use octocrab::{Octocrab, models::pulls::PullRequest, params::State};
use regex::Regex;
use std::collections::HashMap;

pub async fn get_pull_requests_with_issues(
    octocrab: &Octocrab,
    repos: Vec<&str>
) -> HashMap<String, Vec<(PullRequest, Vec<String>)>> {
    let mut repo_prs = HashMap::new();
    // Match GitHub issue linking keywords followed by issue number
    let issue_regex = Regex::new(r"(?i)(close[sd]?|fix(e[sd])?|resolve[sd]?)\s+#(\d+)").unwrap();

    for repo in repos {
        let (owner, repo_name) = repo.split_once('/').expect("Repository must be in format owner/repo");
        let pulls = match octocrab.pulls(owner, repo_name).list()
            .state(State::All)  // Get both open and closed PRs
            .send().await {
            Ok(pulls) => {
                println!("Retrieved {} pulls from API for {}", pulls.items.len(), repo);
                pulls
            },
            Err(e) => {
                eprintln!("Failed to fetch pull requests for {}: {:?}", repo, e);
                continue;
            }
        };
        let mut prs_with_issues = Vec::new();

        for pull in pulls {
            let mut issues = Vec::new();
            if let Some(body) = &pull.body {
                for cap in issue_regex.captures_iter(body) {
                    if let Some(issue) = cap.get(3) {  // Group 3 contains the issue number
                        issues.push(issue.as_str().to_string());
                    }
                }
            }
            prs_with_issues.push((pull, issues));
        }
        repo_prs.insert(repo.to_string(), prs_with_issues);
    }

    repo_prs
}
