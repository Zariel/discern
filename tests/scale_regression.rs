use std::fs;
use std::path::{Path, PathBuf};

use discern::application::config::ValidatedRuntimeConfig;
use discern::application::repository::{
    ReleaseCommandRepository, ReleaseListQuery, ReleaseRepository,
};
use discern::config::AppConfig;
use discern::domain::artist::Artist;
use discern::domain::release::{Release, ReleaseEdition};
use discern::domain::release_group::{ReleaseGroup, ReleaseGroupKind};
use discern::infrastructure::sqlite::{SqliteRepositories, SqliteRepositoryContext};
use discern::support::pagination::PageRequest;

#[test]
fn large_library_release_page_fixture_stays_stable() {
    let root = temp_root("scale-release-page");
    let config = test_config(&root);
    let repository = open_repositories(&config);

    for index in 0..120u16 {
        let artist = Artist {
            id: discern::support::ids::ArtistId::new(),
            name: format!("Artist {index:03}"),
            sort_name: Some(format!("Artist {index:03}")),
            musicbrainz_artist_id: None,
        };
        repository
            .create_artist(&artist)
            .expect("artist should persist");

        let group = ReleaseGroup {
            id: discern::support::ids::ReleaseGroupId::new(),
            primary_artist_id: artist.id.clone(),
            title: format!("Release {index:03}"),
            kind: ReleaseGroupKind::Album,
            musicbrainz_release_group_id: None,
        };
        repository
            .create_release_group(&group)
            .expect("release group should persist");

        repository
            .create_release(&Release {
                id: discern::support::ids::ReleaseId::new(),
                release_group_id: group.id,
                primary_artist_id: artist.id,
                title: format!("Release {index:03}"),
                musicbrainz_release_id: None,
                discogs_release_id: None,
                edition: ReleaseEdition {
                    edition_title: None,
                    disambiguation: None,
                    country: None,
                    label: None,
                    catalog_number: None,
                    release_date: None,
                },
            })
            .expect("release should persist");
    }

    let page = repository
        .list_releases(&ReleaseListQuery {
            release_group_id: None,
            text: Some("Release".to_string()),
            page: PageRequest::new(25, 50),
        })
        .expect("release page should load");

    assert_eq!(
        render_release_page_fixture(&page),
        include_str!("golden/large_library_release_page.txt")
    );

    let _ = fs::remove_dir_all(root);
}

fn render_release_page_fixture(page: &discern::support::pagination::Page<Release>) -> String {
    let first_title = page
        .items
        .first()
        .map(|release| release.title.clone())
        .unwrap_or_default();
    let last_title = page
        .items
        .last()
        .map(|release| release.title.clone())
        .unwrap_or_default();

    format!(
        "limit={}\noffset={}\ntotal={}\nfirst_title={}\nlast_title={}\n",
        page.request.limit, page.request.offset, page.total, first_title, last_title
    )
}

fn temp_root(prefix: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).expect("temp root should create");
    root
}

fn test_config(root: &Path) -> ValidatedRuntimeConfig {
    let mut config = AppConfig::default();
    config.storage.sqlite_path = root.join("discern.db");
    config.storage.managed_library_root = root.join("library");
    config.storage.watch_directories[0].path = root.join("incoming");
    ValidatedRuntimeConfig::from_validated_app_config(&config)
}

fn open_repositories(config: &ValidatedRuntimeConfig) -> SqliteRepositories {
    fs::create_dir_all(&config.storage.managed_library_root)
        .expect("managed library root should create");
    for watcher in &config.storage.watch_directories {
        fs::create_dir_all(&watcher.path).expect("watch directory should create");
    }
    let context = SqliteRepositoryContext::open(config.storage.sqlite_path.clone())
        .expect("context should open");
    context.ensure_schema().expect("schema should initialize");
    SqliteRepositories::new(context)
}
