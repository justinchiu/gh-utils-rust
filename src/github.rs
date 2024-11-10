use octocrab::{Octocrab, models::pulls::PullRequest, params::State};

// Documentation for PullRequestHandler: https://docs.rs/octocrab/latest/octocrab/pulls/struct.PullRequestHandler.html
use regex::Regex;
use std::collections::HashMap;

pub async fn get_pull_requests_with_issues(
    octocrab: &Octocrab,
    repos: Vec<&str>
) -> HashMap<String, Vec<(PullRequest, Vec<String>)>> {
    let mut repo_prs = HashMap::new();
    // Match GitHub issue linking keywords followed by issue number
    let issue_regex = Regex::new(r"(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|#)(\d+)|https?://github\.com/[^/]+/[^/]+/issues/(\d+)").unwrap();

    for repo in repos {
        let (owner, repo_name) = repo.split_once('/').expect("Repository must be in format owner/repo");
        let mut all_pulls = Vec::new();
        let mut page = octocrab.pulls(owner, repo_name)
            .list()
            .state(State::All)
            .per_page(100)
            .send()
            .await;

        while let Ok(mut current_page) = page {
            all_pulls.extend(current_page.take_items());
            if let Some(next_page) = octocrab.get_page::<PullRequest>(&current_page.next).await.unwrap_or(None) {
                page = Ok(next_page);
            } else {
                break;
            }
        }

        if all_pulls.is_empty() {
            eprintln!("Failed to fetch pull requests for {}", repo);
            continue;
        }

        println!("Retrieved {} pulls from API for {}", all_pulls.len(), repo);
        let mut prs_with_issues = Vec::new();

        for pull in all_pulls {
            let mut issues = Vec::new();
            // Check PR title for issue references
            if let Some(title) = &pull.title {
                for cap in issue_regex.captures_iter(title) {
                    if let Some(issue) = cap.get(1) {
                        issues.push(issue.as_str().to_string());
                    }
                    } else if let Some(url_issue) = cap.get(2) {
                        issues.push(url_issue.as_str().to_string());
                    }
                }
            }
            
            // Check PR body for issue references
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

    repo_prs
}
