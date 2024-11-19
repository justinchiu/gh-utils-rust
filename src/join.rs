use git2::Repository;
use octocrab::models::{issues::Issue, pulls::PullRequest, repos::RepoCommit};
use std::collections::HashMap;
use std::path::Path;

pub struct RepoAnalysis {
    pub repo_name: String,
    pub issues: Vec<Issue>,
    pub prs_with_issues: Vec<(PullRequest, Vec<String>)>,
    pub commits_with_issues: Vec<(RepoCommit, Vec<String>)>,
    pub local_repo: Option<Repository>,
}

impl RepoAnalysis {
    pub fn new(repo_name: String) -> Self {
        RepoAnalysis {
            repo_name,
            issues: Vec::new(),
            prs_with_issues: Vec::new(),
            commits_with_issues: Vec::new(),
            local_repo: None,
        }
    }

    pub fn load_local_repo(&mut self) {
        let base_path = Path::new("repos");
        let (owner, repo_name) = self.repo_name
            .split_once('/')
            .expect("Repository must be in format owner/repo");
        
        let repo_path = base_path.join(format!("{}__{}", owner, repo_name));
        self.local_repo = Repository::open(repo_path).ok();
    }
}

pub fn align_repo_data(
    repos: &[String],
    repo_issues: &HashMap<String, Vec<Issue>>,
    repo_prs: &HashMap<String, Vec<(PullRequest, Vec<String>)>>,
    repo_commits: &HashMap<String, Vec<(RepoCommit, Vec<String>)>>,
) -> Vec<RepoAnalysis> {
    let mut analyses = Vec::new();

    for repo_name in repos {
        let mut analysis = RepoAnalysis::new(repo_name.clone());
        
        // Load issues
        if let Some(issues) = repo_issues.get(repo_name) {
            analysis.issues = issues.clone();
        }

        // Load PRs with their linked issues
        if let Some(prs) = repo_prs.get(repo_name) {
            analysis.prs_with_issues = prs.clone();
        }

        // Load commits with their linked issues
        if let Some(commits) = repo_commits.get(repo_name) {
            analysis.commits_with_issues = commits.clone();
        }

        // Try to load local repository
        analysis.load_local_repo();

        analyses.push(analysis);
    }

    analyses
}

pub fn print_analysis_summary(analyses: &[RepoAnalysis]) {
    for analysis in analyses {
        println!("\nRepository: {}", analysis.repo_name);
        println!("Total issues: {}", analysis.issues.len());
        println!("PRs with linked issues: {}", analysis.prs_with_issues.len());
        println!("Commits with linked issues: {}", analysis.commits_with_issues.len());
        
        if analysis.local_repo.is_some() {
            println!("Local repository: Available");
        } else {
            println!("Local repository: Not found");
        }

        // Print some issue-PR relationships
        for (pr, linked_issues) in &analysis.prs_with_issues {
            if !linked_issues.is_empty() {
                println!(
                    "PR #{} links to issues: {:?}",
                    pr.number,
                    linked_issues
                );
            }
        }
    }
}
