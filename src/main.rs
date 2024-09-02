use csv::Reader;
use csv::StringRecord;
use std::path::Path;
use std::time::Instant;
use std::io::Read;
use git2::Repository;
use walkdir::{DirEntry, WalkDir};
use octocrab::Octocrab;
use futures::future::try_join_all;
use std::sync::Arc;

#[derive(Default,Debug)]
struct Stats {
    num_lines: usize,
    num_files: usize,
    has_tests: bool,
    has_docs: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN env variable is required");
    let octocrab = Arc::new(Octocrab::builder().personal_token(token).build()?);

    let file_path = Path::new("mydata/data.csv");
    let file = std::fs::File::open(file_path)?;
    let mut reader = Reader::from_reader(file);


    if !std::path::Path::new("./repos").exists() {
        std::fs::create_dir("./repos")?;
    }
    let headers = reader.headers()?.clone();
    println!("Header: {:?}", headers);
    
    let records: Vec<StringRecord> = reader.records().take(5).filter_map(Result::ok).collect();
    
    let futures = records.iter().map(|record| {
        let record = record.clone();
        let octocrab = Arc::clone(&octocrab);
        async move {
            println!("Record: {:?}", record);
            let stats_record = record.clone();
            tokio::spawn(async move {
                if let Err(e) = get_stats(&stats_record).await {
                    eprintln!("Error in get_stats: {:?}", e);
                }
            });
            get_metadata(&record, &octocrab).await
        }
    });

    try_join_all(futures).await?;

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

async fn get_stats(record: &StringRecord) -> Result<(), Box<dyn std::error::Error>> {
    // clone repo
    let fullrepo = record.get(0).unwrap();
    let url = format!("https://github.com/{fullrepo}");
    let (owner, reponame) = get_owner_repo(&record);
    let repo_path = format!("./repos/{reponame}");
    if std::path::Path::new(&repo_path).exists() {
        println!("repo already exists at {repo_path}");
    } else {
        println!("cloning {owner}/{reponame} to {repo_path}");
        match Repository::clone(&url, &repo_path) {
            Ok(repo) => repo,
            Err(e) => panic!("Failed to clone {}: {}", url, e),
        };
    }

    // process files
    let mut stats = Stats::default();

    for entry in WalkDir::new(repo_path.clone()).into_iter().filter_entry(|e| !is_hidden(e)).filter_map(|e| e.ok()) {
        let path = entry.path();

        // Only process files (not directories)
        if path.is_file() {
            // Read the file content as a string
            let content = match read_file_to_string(path) {
                Ok(content) => content,
                Err(e) => {
                    eprintln!("Error reading file {:?}: {}", path, e);
                    String::new()
                },
            };
            // println!("{}", content);
            stats.num_files += 1;
            stats.num_lines += content.lines().count();
            if path.to_str().unwrap_or("").contains("test") {
                stats.has_tests = true;
            }
            if path.to_str().unwrap_or("").contains("doc") || content.contains("/**") {
                stats.has_docs = true;
            }
        }
    }
    
    println!("Stats for {}: {:?}", fullrepo, stats);
    
    //  cleanup repo
    std::fs::remove_dir_all(repo_path)?;
    Ok(())
}

async fn get_metadata(record: &StringRecord, octocrab: &Octocrab) -> Result<(), Box<dyn std::error::Error>> {
    let (owner, reponame) = get_owner_repo(&record);
    let content = octocrab.repos(owner, reponame).get_content().send().await?;
    let num_files = content.items.len();
    println!("{num_files} files/dirs in the repo {owner}/{reponame}");

    println!("{:?}", content.items[0]);
    Ok(())
}

fn get_owner_repo(record: &StringRecord) -> (&str, &str) {
    let parts: Vec<&str> = record.get(0).unwrap().split('/').collect();
    if parts.len() != 2 {
        panic!("Invalid input format. Expected 'owner/repo'.");
    }
    (parts[0], parts[1])
}

// Utility function to read a file's contents into a string
fn read_file_to_string<P: AsRef<Path>>(path: P) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}
