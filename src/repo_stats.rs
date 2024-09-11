use csv::StringRecord;
use git2::Repository;
use walkdir::{DirEntry, WalkDir};
use std::path::Path;
use tokio::fs;
use std::error::Error;
use std::ffi::OsStr;

pub async fn get_stats(record: &StringRecord) -> Result<(u64, u64), Box<dyn Error + Send + Sync>> {
    let fullrepo = record.get(1).unwrap();
    let url = format!("https://github.com/{fullrepo}");

    let (owner, reponame) = get_owner_repo(&record);
    let repo_path = format!("./repos/{reponame}");
    
    if Path::new(&repo_path).exists() {
        println!("repo already exists at {repo_path}");
    } else {
        println!("cloning {owner}/{reponame} to {repo_path}");
        Repository::clone(&url, &repo_path)?;
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

    println!("Repo: {}, Total Lines: {}, Total Comment Lines: {}", reponame, total_lines, total_comment_lines);

    fs::remove_dir_all(repo_path).await?;
    Ok((total_lines as u64, total_comment_lines as u64))
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
