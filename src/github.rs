use octocrab::{Octocrab, models::pulls::PullRequest};
use regex::Regex;
use std::collections::HashMap;

pub async fn get_pull_requests_with_issues(
    octocrab: &Octocrab,
    repos: Vec<&str>
) -> HashMap<String, Vec<(PullRequest, Vec<String>)>> {
    let mut repo_prs = HashMap::new();
    let issue_regex = Regex::new(r"#(\d+)").unwrap();

    for repo in repos {
        let (owner, repo_name) = repo.split_once('/').expect("Repository must be in format owner/repo");
        let pulls = match octocrab.pulls(owner, repo_name).list().send().await {
            Ok(pulls) => pulls,
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
                    if let Some(issue) = cap.get(1) {
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
