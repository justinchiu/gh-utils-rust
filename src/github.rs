use octocrab::{models::pulls::PullRequest, models::repos::Commit, params::State, Octocrab};
use std::time::Instant;

// Documentation for PullRequestHandler: https://docs.rs/octocrab/latest/octocrab/pulls/struct.PullRequestHandler.html
use regex::Regex;
use std::collections::HashMap;

pub async fn get_pull_requests_with_issues(
    octocrab: &Octocrab,
    repos: Vec<&str>,
) -> HashMap<String, Vec<(PullRequest, Vec<String>)>> {
    let mut repo_prs = HashMap::new();
    // Match GitHub issue linking keywords followed by issue number
    let keyword_issue_regex =
        Regex::new(r"(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|#)(\d+)").unwrap();
    // Match GitHub issue URLs
    let url_issue_regex = Regex::new(r"https?://github\.com/[^/]+/[^/]+/issues/(\d+)").unwrap();

    for repo in repos {
        let (owner, repo_name) = repo
            .split_once('/')
            .expect("Repository must be in format owner/repo");
        let start_time = Instant::now();
        let all_pulls = fetch_all_pull_requests(octocrab, owner, repo_name).await;
        let duration = start_time.elapsed();
        println!(
            "Time taken to fetch pull requests for {}: {:?}",
            repo, duration
        );

        if all_pulls.is_empty() {
            eprintln!("Failed to fetch pull requests for {}", repo);
            continue;
        }

        println!("Retrieved {} pulls from API for {}", all_pulls.len(), repo);
        let mut prs_with_issues = Vec::new();
        for pull in all_pulls {
            let issues = extract_issues_from_pr(&pull, &keyword_issue_regex, &url_issue_regex);
            prs_with_issues.push((pull, issues));
        }

        // Fetch all commits and apply issue URLs
        let start_time = Instant::now();
        let all_commits = fetch_all_commits(octocrab, owner, repo_name).await;
        let duration = start_time.elapsed();
        println!(
            "Time taken to fetch commits for {}: {:?}",
            repo, duration
        );
        println!("Retrieved {} commits from API for {}", all_commits.len(), repo);

        repo_prs.insert(repo.to_string(), prs_with_issues);
    }
    repo_prs
}

async fn fetch_all_pull_requests(
    octocrab: &Octocrab,
    owner: &str,
    repo_name: &str,
) -> Vec<PullRequest> {
    let mut page = octocrab
        .pulls(owner, repo_name)
        .list()
        .state(State::All)
        .per_page(100)
        .send()
        .await;

    let mut all_pulls = Vec::new();
    while let Ok(mut current_page) = page {
        all_pulls.extend(current_page.take_items());
        if let Some(next_page) = octocrab
            .get_page::<PullRequest>(&current_page.next)
            .await
            .unwrap_or(None)
        {
            page = Ok(next_page);
        } else {
            break;
        }
    }
    all_pulls
}

async fn fetch_all_commits(
    octocrab: &Octocrab,
    owner: &str,
    repo_name: &str,
) -> Vec<octocrab::models::repos::RepoCommit> {
    let mut all_commits = Vec::new();
    let mut page = octocrab
        .repos(owner, repo_name)
        .list_commits()
        .per_page(100)
        .send()
        .await;

    while let Ok(mut current_page) = page {
        all_commits.extend(current_page.take_items());
        page = match current_page.next {
            Some(url) => octocrab.get_page::<octocrab::models::repos::RepoCommit>(&url).await,
            None => break,
        }
    }
    all_commits
}

fn extract_issues_from_pr(
    pull: &PullRequest,
    keyword_issue_regex: &Regex,
    url_issue_regex: &Regex,
) -> Vec<String> {
    let mut issues = Vec::new();
    // Check PR title for issue references
    if let Some(title) = &pull.title {
        for cap in keyword_issue_regex.captures_iter(title) {
            if let Some(issue) = cap.get(1) {
                issues.push(issue.as_str().to_string());
            }
        }
        for cap in url_issue_regex.captures_iter(title) {
            if let Some(issue) = cap.get(1) {
                issues.push(issue.as_str().to_string());
            }
        }
    }
    // Check PR body for issue references
    if let Some(body) = &pull.body {
        for cap in keyword_issue_regex.captures_iter(body) {
            if let Some(issue) = cap.get(1) {
                issues.push(issue.as_str().to_string());
            }
        }
        for cap in url_issue_regex.captures_iter(body) {
            if let Some(issue) = cap.get(1) {
                issues.push(issue.as_str().to_string());
            }
        }
    }
    issues
}
