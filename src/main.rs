mod github;

use github::get_pull_requests_with_issues;
use octocrab::Octocrab;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let repos = vec!["msiemens/tinydb"];
    println!("Fetching pull requests for repositories: {:?}", repos);
    
    let repo_prs = get_pull_requests_with_issues(&octocrab, repos).await;
    
    if repo_prs.is_empty() {
        println!("No repositories found with pull requests.");
        return Ok(());
    }

    for (repo, prs) in repo_prs.iter() {
        println!("\nRepository: {}", repo);
        if prs.is_empty() {
            println!("No pull requests found.");
            continue;
        }

        println!("Found {} pull requests", prs.len());
        for (pr, issues) in prs {
            println!("\nPR #{}: {}", 
                pr.number,
                pr.title.as_deref().unwrap_or("No title")
            );
            if !issues.is_empty() {
                println!("Related issues: {:?}", issues);
            }
        }
    }

    Ok(())
}
