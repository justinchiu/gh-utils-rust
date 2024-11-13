mod github;

use csv::Reader;
use github::{get_commits_with_issues, get_pull_requests_with_issues};
use octocrab::Octocrab;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::time::Instant;

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
    let file = File::open("mydata/repos.csv")?;
    let mut rdr = Reader::from_reader(file);

    let mut repos = Vec::new();
    for result in rdr.deserialize() {
        let record: RepoData = result?;
        println!("{:?}", &record.repo_name);
        repos.push(record.repo_name);
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

    if repo_prs.is_empty() && repo_commits.is_empty() {
        println!("No repositories found with pull requests or commits.");
        return Ok(());
    }

    // Print pull requests
    for (repo, prs) in repo_prs.iter() {
        println!("\nRepository: {}", repo);
        if prs.is_empty() {
            println!("No pull requests found.");
        } else {
            println!("Found {} pull requests", prs.len());
            for (pr, issues) in prs {
                println!(
                    "\nPR #{}: {}",
                    pr.number,
                    pr.title.as_deref().unwrap_or("No title")
                );
                if !issues.is_empty() {
                    println!("Related issues: {:?}", issues);
                }
            }
        }
    }

    // Print commits
    for (repo, commits) in repo_commits.iter() {
        println!("\nRepository: {} (Commits)", repo);
        if commits.is_empty() {
            println!("No commits found with issue references.");
        } else {
            println!("Found {} commits with issue references", commits.len());
            for (commit, issues) in commits {
                println!(
                    "\nCommit {}: {}",
                    &commit.sha[..7],
                    commit.commit.message.lines().next().unwrap_or("No message")
                );
                println!("Related issues: {:?}", issues);
            }
        }
    }

    Ok(())
}
