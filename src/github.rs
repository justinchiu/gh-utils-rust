use octocrab::{Octocrab, models::pulls::PullRequest, params::State};
use regex::Regex;
use std::collections::HashMap;

pub async fn get_pull_requests_with_issues(
    octocrab: &Octocrab,
    repos: Vec<&str>
) -> HashMap<String, Vec<(PullRequest, Vec<String>)>> {
    let mut repo_prs = HashMap::new();
    // Match GitHub issue linking keywords followed by issue number
    let issue_regex = Regex::new(r"(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|#)(\d+)").unwrap();

    for repo in repos {
        let (owner, repo_name) = repo.split_once('/').expect("Repository must be in format owner/repo");
        let initial_page = octocrab.pulls(owner, repo_name)
            .list()
            .state(State::All)  // Get both open and closed PRs
            .per_page(100)  // Maximum allowed per page
            .send()
            .await;

        let all_pulls = match initial_page {
            Ok(mut page) => {
                let mut pulls = page.items;
                while let Some(next_url) = page.next {
                    match octocrab.get_page::<PullRequest>(&Some(next_url)).await {
                        Ok(Some(next_page)) => {
                            pulls.extend(next_page.items.clone().into_iter());
                            page = next_page;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("Error fetching next page: {:?}", e);
                            break;
                        }
                    }
                }
                println!("Retrieved {} pulls from API for {}", pulls.len(), repo);
                pulls
            },
            Err(e) => {
                eprintln!("Failed to fetch pull requests for {}: {:?}", repo, e);
                continue;
            }
        };
        let mut prs_with_issues = Vec::new();

        for pull in all_pulls {
            let mut issues = Vec::new();
            // Check PR title for issue references
            if let Some(title) = &pull.title {
                for cap in issue_regex.captures_iter(title) {
                    if let Some(issue) = cap.get(1) {
                        issues.push(issue.as_str().to_string());
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
    }

    repo_prs
}
