use csv::Reader;
use csv::StringRecord;
use std::path::Path;
use std::time::Instant;
use octocrab::Octocrab;
use git2::Repository;


fn get_owner_repo(record: &StringRecord) -> (&str, &str) {
    let parts: Vec<&str> = record.get(0).unwrap().split('/').collect();
    if parts.len() != 2 {
        panic!("Invalid input format. Expected 'owner/repo'.");
    }
    (parts[0], parts[1])
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let token = std::env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN env variable is required");
    let octocrab = Octocrab::builder().personal_token(token).build()?;

    let file_path = Path::new("mydata/data.csv");
    let file = std::fs::File::open(file_path)?;
    let mut reader = Reader::from_reader(file);


    std::fs::create_dir("./repos")?;
    let headers = reader.headers()?.clone();
    println!("Header: {:?}", headers);
    for (index, result) in reader.records().enumerate() {
        let record = result?;
        println!("Record: {:?}", record);
        let fullrepo = record.get(0).unwrap();
        let url = "https://github.com/{fullrepo}";
        let (owner, reponame) = get_owner_repo(&record);
        println!("{owner} {reponame}");
        let repo = match Repository::clone(url, "./repos/{reponame}") {
            Ok(repo) => repo,
            Err(e) => panic!("failed to clone: {}", e),
        };



        let content = octocrab.repos(owner, reponame).get_content().send().await?;
        let num_files = content.items.len();
        println!("{num_files} files/dirs in the repo {owner}/{reponame}");

        println!("{:?}", content.items[0]);

        if index == 5 {
            break;
        }
    }

    let end = Instant::now();
    let duration = (end - start).as_secs_f64();
    println!("Duration: {duration}");

    Ok(())
}
