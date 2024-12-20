use git2::Repository;
use octocrab::{
    models::{issues::Issue, pulls::PullRequest, repos::RepoCommit},
    params::State,
    Octocrab,
};
use std::path::Path;
use std::string::String;

// Documentation for PullRequestHandler: https://docs.rs/octocrab/latest/octocrab/pulls/struct.PullRequestHandler.html
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use std::collections::HashMap;

pub async fn get_pull_requests_with_issues(
    octocrab: &Octocrab,
    repos: &Vec<String>,
) -> HashMap<String, Vec<(PullRequest, Vec<String>)>> {
    let mut repo_prs = HashMap::new();
    // Match GitHub issue linking keywords followed by issue number
    let keyword_issue_regex =
        Regex::new(r"(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|#)(\d+)").unwrap();
    // Match GitHub issue URLs
    let url_issue_regex = Regex::new(r"https?://github\.com/[^/]+/[^/]+/issues/(\d+)").unwrap();

    let pb = ProgressBar::new(repos.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    for repo in pb.wrap_iter(repos.iter()) {
        pb.set_message(format!("Processing {}", repo));
        let (owner, repo_name) = repo
            .split_once('/')
            .expect("Repository must be in format owner/repo");
        let all_pulls = fetch_all_pull_requests(octocrab, owner, repo_name).await;

        if all_pulls.is_empty() {
            eprintln!("Failed to fetch pull requests for {}", repo);
            continue;
        }

        let mut prs_with_issues = Vec::new();
        for pull in all_pulls {
            let issues = extract_issues_from_pr(&pull, &keyword_issue_regex, &url_issue_regex);
            prs_with_issues.push((pull, issues));
        }

        repo_prs.insert(repo.to_string(), prs_with_issues);
    }
    pb.finish_with_message("Completed fetching pull requests");
    repo_prs
}

async fn fetch_all_pull_requests(
    octocrab: &Octocrab,
    owner: &str,
    repo_name: &str,
) -> Vec<PullRequest> {
    let mut all_pulls = Vec::new();
    let result_page = octocrab
        .pulls(owner, repo_name)
        .list()
        .state(State::All)
        .per_page(100)
        .send()
        .await;
    let mut page = match result_page {
        Ok(x) => x,
        Err(e) => {
            eprintln!("Error fetching PRs: {}", e);
            return all_pulls;
        }
    };

    loop {
        all_pulls.extend(page.take_items());
        match octocrab.get_page(&page.next).await {
            Ok(Some(next_page)) => page = next_page,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error fetching next page: {}", e);
                break;
            }
        }
    }
    all_pulls
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

pub async fn get_commits_with_issues(
    octocrab: &Octocrab,
    repos: &Vec<String>,
) -> HashMap<String, Vec<(RepoCommit, Vec<String>)>> {
    let mut repo_commits = HashMap::new();
    // Match GitHub issue linking keywords followed by issue number
    let keyword_issue_regex =
        Regex::new(r"(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|#)(\d+)").unwrap();
    // Match GitHub issue URLs
    let url_issue_regex = Regex::new(r"https?://github\.com/[^/]+/[^/]+/issues/(\d+)").unwrap();

    let pb = ProgressBar::new(repos.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    for repo in pb.wrap_iter(repos.iter()) {
        pb.set_message(format!("Processing {}", repo));
        let (owner, repo_name) = repo
            .split_once('/')
            .expect("Repository must be in format owner/repo");
        // Fetch all commits and apply issue URLs
        let all_commits = fetch_all_commits(octocrab, owner, repo_name).await;

        let mut commits_with_issues = Vec::new();
        // Process commits for issues
        for commit in all_commits {
            let commit_issues =
                extract_issues_from_commit(&commit, &keyword_issue_regex, &url_issue_regex);
            if !commit_issues.is_empty() {
                println!("Found issues in commit {}: {:?}", commit.sha, commit_issues);
                commits_with_issues.push((commit, commit_issues));
            }
        }

        repo_commits.insert(repo.to_string(), commits_with_issues);
    }
    pb.finish_with_message("Completed fetching commits");
    repo_commits
}

fn extract_issues_from_commit(
    commit: &RepoCommit,
    keyword_issue_regex: &Regex,
    url_issue_regex: &Regex,
) -> Vec<String> {
    let mut issues = Vec::new();

    let message = &commit.commit.message;
    // Check commit message for issue references
    for cap in keyword_issue_regex.captures_iter(&message) {
        if let Some(issue) = cap.get(1) {
            issues.push(issue.as_str().to_string());
        }
    }
    for cap in url_issue_regex.captures_iter(&message) {
        if let Some(issue) = cap.get(1) {
            issues.push(issue.as_str().to_string());
        }
    }
    issues
}

async fn fetch_all_commits(octocrab: &Octocrab, owner: &str, repo_name: &str) -> Vec<RepoCommit> {
    let mut all_commits = Vec::new();
    let result_page = octocrab
        .repos(owner, repo_name)
        .list_commits()
        .per_page(100)
        .send()
        .await;
    let mut page = match result_page {
        Ok(x) => x,
        Err(e) => {
            eprintln!("Error fetching initial commits: {}", e);
            return all_commits;
        }
    };

    loop {
        all_commits.extend(page.take_items());
        match octocrab.get_page(&page.next).await {
            Ok(Some(next_page)) => page = next_page,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error fetching next commit page: {}", e);
                break;
            }
        }
    }
    all_commits
}

pub async fn get_all_issues(
    octocrab: &Octocrab,
    repos: &Vec<String>,
) -> HashMap<String, Vec<Issue>> {
    let mut repo_issues = HashMap::new();

    let pb = ProgressBar::new(repos.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    for repo in pb.wrap_iter(repos.iter()) {
        pb.set_message(format!("Processing {}", repo));
        let (owner, repo_name) = repo
            .split_once('/')
            .expect("Repository must be in format owner/repo");

        let mut all_issues = Vec::new();
        let result_page = octocrab
            .issues(owner, repo_name)
            .list()
            .state(State::All)
            .per_page(100)
            .send()
            .await;

        let mut page = match result_page {
            Ok(x) => x,
            Err(e) => {
                eprintln!("Error fetching issues for {}: {}", repo, e);
                continue;
            }
        };

        loop {
            all_issues.extend(page.take_items());
            match octocrab.get_page(&page.next).await {
                Ok(Some(next_page)) => page = next_page,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("Error fetching next page: {}", e);
                    break;
                }
            }
        }

        if !all_issues.is_empty() {
            repo_issues.insert(repo.to_string(), all_issues);
        }
    }

    pb.finish_with_message("Completed fetching issues");
    repo_issues
}

pub fn clone_repositories(repos: &Vec<String>) -> Result<(), git2::Error> {
    let base_path = Path::new("repos");
    if !base_path.exists() {
        std::fs::create_dir(base_path).expect("Failed to create repos directory");
    }

    let pb = ProgressBar::new(repos.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    for repo in pb.wrap_iter(repos.iter()) {
        pb.set_message(format!("Cloning {}", repo));
        let (owner, repo_name) = repo
            .split_once('/')
            .expect("Repository must be in format owner/repo");

        let repo_path = base_path.join(format!("{}__{}", owner, repo_name));
        if !repo_path.exists() {
            match Repository::clone(
                &format!("https://github.com/{}/{}.git", owner, repo_name),
                &repo_path,
            ) {
                Ok(_) => (),
                Err(e) => eprintln!("Failed to clone {}: {}", repo, e),
            }
        }
    }

    pb.finish_with_message("Completed cloning repositories");
    Ok(())
}
