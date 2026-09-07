fn main() {
    let database_url = load_database_url_from_process_environment();
    println!("configured database endpoint: {database_url}");
}

fn load_database_url_from_process_environment() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "not configured".to_string())
}
