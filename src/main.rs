use csv::Reader;
use csv::StringRecord;
use std::path::Path;
use std::time::Instant;
use std::io::Read;
use git2::Repository;
use walkdir::{DirEntry, WalkDir};
use futures::future::try_join_all;
use std::sync::Arc;
use regex::Regex;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN env variable is required");

    let file_path = Path::new("mydata/data.csv");
    let file = std::fs::File::open(file_path)?;
    let mut reader = Reader::from_reader(file);


    if !std::path::Path::new("./repos").exists() {
        std::fs::create_dir("./repos")?;
    }
    let headers = reader.headers()?.clone();
    println!("Header: {:?}", headers);
    
    let records: Vec<StringRecord> = reader.records().take(5).filter_map(Result::ok).collect();
    
    let start = Instant::now();
    for record in records {
        let record = record.clone();
        println!("Record: {:?}", record);
        get_stats(&record);

    }

    let end = Instant::now();
    let duration = (end - start).as_secs_f32();
    println!("Duration: {duration} secs");

    Ok(())
}

fn get_stats(record: &StringRecord) -> Result<(), Box<dyn std::error::Error>> {
    // clone repo
    let fullrepo = record.get(1).unwrap();
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

    for entry in WalkDir::new(repo_path.clone())
        .into_iter()
        .filter_entry(|e| !is_hidden(e))
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "py"))
    {
        let path = entry.path();

        // Read the file content as a string
        let content = match read_file_to_string(path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error reading file {:?}: {}", path, e);
                String::new()
            },
        };
        println!("Python file: {:?}", path);
        println!("{}", content);
    }
   
    //  cleanup repo
    std::fs::remove_dir_all(repo_path)?;
    Ok(())
}

fn get_owner_repo(record: &StringRecord) -> (&str, &str) {
    let repo = record.get(1).unwrap();
    let parts: Vec<&str> = repo.split('/').collect();
    if parts.len() != 2 {
        panic!("Invalid input format. Expected 'owner/repo'. Got {:?}", repo);
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
