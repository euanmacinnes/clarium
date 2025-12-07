use super::*;

#[test]
fn precedence_global_fs_folder() {
    let global = GlobalFilestoreConfig::default();
    let mut fs = FilestoreConfig::default();
    fs.git_branch = Some("dev".into());
    fs.git_remote = Some("git@github.com:org/root.git".into());
    // change ACL deny ttl only at FS level
    fs.acl_cache_ttl_deny_ms = Some(5000);

    let folder = FolderGitOverride { git_remote: Some("git@github.com:org/docs.git".into()), git_branch: None, git_mode: Some("plumbing_only".into()) };

    let eff = EffectiveConfig::from_layers(&global, &fs, Some(&folder));
    assert_eq!(eff.git_remote.as_deref(), Some("git@github.com:org/docs.git"));
    assert_eq!(eff.git_branch.as_deref(), Some("dev"));
    assert_eq!(eff.git_mode, "plumbing_only");
    assert_eq!(eff.acl_cache_ttl_deny_ms, 5000);
    assert_eq!(eff.html_description_max_bytes, 32768);
}
