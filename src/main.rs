mod github;

use csv::Reader;
use github::{get_commits_with_issues, get_pull_requests_with_issues};
use octocrab::Octocrab;
use serde::Deserialize;
use std::fs::File;
use std::io::Write;

#[derive(Debug, Deserialize)]
struct RepoData {
    repo_name: String,
    stars: i32,
    forks: i32,
    issues: i32,
    license_type: String,
    num_files: i32,
    num_python_files: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get repos
    let file = File::open("mydata/python.csv")?;
    let mut rdr = Reader::from_reader(file);

    let mut repos = Vec::new();
    for result in rdr.deserialize() {
        let record: RepoData = result?;
        println!("{:?}", &record.repo_name);
        repos.push(record.repo_name);
    }

    println!("Processing {} repos", repos.len());
    
    // Clone repositories
    println!("Cloning repositories...");
    if let Err(e) = github::clone_repositories(&repos) {
        eprintln!("Error cloning repositories: {}", e);
    }

    // Check if token exists and print status
    let token = match std::env::var("GITHUB_TOKEN") {
        Ok(token) => {
            println!("✓ Found GitHub token");
            token
        }
        Err(_) => {
            eprintln!("Error: GITHUB_TOKEN environment variable is not set");
            eprintln!("Please set it with: export GITHUB_TOKEN=your_github_token");
            std::process::exit(1);
        }
    };

    println!("Connecting to GitHub...");
    let octocrab = Octocrab::builder()
        .personal_token(token)
        .build()
        .map_err(|e| format!("Failed to create GitHub client: {}", e))?;

    let repo_prs = get_pull_requests_with_issues(&octocrab, &repos).await;
    let repo_commits = get_commits_with_issues(&octocrab, &repos).await;
    let repo_issues = get_all_issues(&octocrab, &repos).await;

    if repo_prs.is_empty() && repo_commits.is_empty() && repo_issues.is_empty() {
        println!("No repositories found with pull requests, commits, or issues.");
        return Ok(());
    }

    // Save pull requests to JSON file
    let prs_json = serde_json::to_string_pretty(&repo_prs)?;
    let mut prs_file = File::create("pull_requests.json")?;
    prs_file.write_all(prs_json.as_bytes())?;
    println!("Saved pull requests data to pull_requests.json");

    // Save commits to JSON file
    let commits_json = serde_json::to_string_pretty(&repo_commits)?;
    let mut commits_file = File::create("commits.json")?;
    commits_file.write_all(commits_json.as_bytes())?;
    println!("Saved commits data to commits.json");

    // Save issues to JSON file
    let issues_json = serde_json::to_string_pretty(&repo_issues)?;
    let mut issues_file = File::create("issues.json")?;
    issues_file.write_all(issues_json.as_bytes())?;
    println!("Saved issues data to issues.json");

    Ok(())
}
