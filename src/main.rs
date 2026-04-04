use discern::config::AppConfig;
use discern::runtime::bootstrap;

fn main() {
    let runtime = bootstrap(AppConfig::default()).expect("default runtime config should be valid");
    println!("{}", runtime.startup_summary());
}
