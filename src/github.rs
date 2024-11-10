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
        let mut all_pulls = Vec::new();
        let mut current_page = octocrab.pulls(owner, repo_name)
            .list()
            .state(State::All)  // Get both open and closed PRs
            .per_page(100)  // Maximum allowed per page
            .send()
            .await;

        match current_page {
            Ok(mut page) => {
                all_pulls.extend(page.items);
                while page.next.is_some() {
                    match octocrab.get_page(&page.next.unwrap()).await {
                        Ok(next_page) => {
                            all_pulls.extend(next_page.items);
                            page = next_page;
                        }
                        Err(e) => {
                            eprintln!("Error fetching next page: {:?}", e);
                            break;
                        }
                    }
                }
                println!("Retrieved {} pulls from API for {}", all_pulls.len(), repo);
                all_pulls
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
