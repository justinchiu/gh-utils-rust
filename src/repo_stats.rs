use csv::StringRecord;
use git2::Repository;
use walkdir::{DirEntry, WalkDir};
use std::path::Path;
use tokio::fs;
use std::error::Error;
use std::ffi::OsStr;
use std::process::Command;

pub async fn get_stats(record: &StringRecord, github_token: Option<&str>) -> Result<(u64, u64, u64), Box<dyn Error + Send + Sync>> {
    let fullrepo = record.get(1).unwrap();
    let url = format!("https://github.com/{fullrepo}");

    let (owner, reponame) = get_owner_repo(record);
    let repo_path = format!("./repos/{reponame}");
    
    if Path::new(&repo_path).exists() {
        println!("repo already exists at {repo_path}");
    } else {
        println!("cloning {owner}/{reponame} to {repo_path}");
        clone_repository(&url, &repo_path, github_token)?;
    }

    let mut total_lines = 0;
    let mut total_comment_lines = 0;

    let entries = WalkDir::new(repo_path.clone())
        .into_iter()
        .filter_entry(|e| !is_hidden(e))
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file() && is_python_file(e.path()));

    for entry in entries {
        let path = entry.path().to_owned();
        if let Ok(content) = fs::read_to_string(&path).await {
            let line_count = content.lines().count();
            let comment_count = count_comment_lines(&content);
            total_lines += line_count;
            total_comment_lines += comment_count;
            println!("Python File: {:?}, Total Lines: {}, Comment Lines: {}", path, line_count, comment_count);
        }
    }

    let test_count = count_tests(&repo_path)?;

    println!("Repo: {}, Total Lines: {}, Total Comment Lines: {}, Test Count: {}", reponame, total_lines, total_comment_lines, test_count);

    fs::remove_dir_all(repo_path).await?;
    Ok((total_lines as u64, total_comment_lines as u64, test_count))
}

fn get_owner_repo(record: &StringRecord) -> (&str, &str) {
    let repo = record.get(1).unwrap();
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() != 2 {
        panic!("Invalid input format. Expected 'owner/repo'. Got {:?}", repo);
    }
    (parts[0], parts[1])
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}

fn is_python_file(path: &Path) -> bool {
    path.extension()
        .and_then(OsStr::to_str)
        .map(|ext| ext == "py")
        .unwrap_or(false)
}

fn count_comment_lines(content: &str) -> usize {
    content.lines()
        .filter(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("#") || trimmed.starts_with("\"\"\"") || trimmed.starts_with("'''")
        })
        .count()
}
fn clone_repository(url: &str, path: &str, token: Option<&str>) -> Result<Repository, git2::Error> {
    let mut callbacks = git2::RemoteCallbacks::new();
    let mut fetch_options = git2::FetchOptions::new();

    if let Some(token) = token {
        callbacks.credentials(|_url, _username_from_url, _allowed_types| {
            git2::Cred::userpass_plaintext("git", token)
        });
        fetch_options.remote_callbacks(callbacks);
    }

    let mut builder = git2::build::RepoBuilder::new();
    builder.fetch_options(fetch_options);

    builder.clone(url, Path::new(path))
}

fn count_tests(repo_path: &str) -> Result<u64, Box<dyn Error + Send + Sync>> {
    match Command::new("pytest").args(&["--collect-only", "--count", repo_path]).output() {
        Ok(output) => {
            if !output.status.success() {
                eprintln!("Warning: Failed to run pytest. Test count will be 0.");
                return Ok(0);
            }

            let output_str = String::from_utf8(output.stdout)?;
            output_str
                .lines()
                .find(|line| line.contains("collected"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|count| count.parse::<u64>().ok())
                .ok_or_else(|| Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Failed to parse test count",
                )))
        },
        Err(e) => {
            eprintln!("Warning: Failed to execute pytest. Test count will be 0. Error: {}", e);
            Ok(0)
        }
    }
}
