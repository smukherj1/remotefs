use std::collections::BTreeMap;
use std::process::Command;

#[test]
fn workspace_has_only_the_three_architecture_packages() {
    let output = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .expect("run cargo metadata");
    assert!(output.status.success());
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let packages = metadata["packages"].as_array().unwrap();
    let dependencies: BTreeMap<_, Vec<_>> = packages
        .iter()
        .map(|package| {
            let name = package["name"].as_str().unwrap();
            let dependencies = package["dependencies"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|dependency| dependency["name"].as_str())
                .collect();
            (name, dependencies)
        })
        .collect();

    assert_eq!(
        dependencies.keys().copied().collect::<Vec<_>>(),
        ["rfs", "rfs-common", "rfsd"]
    );
    assert_eq!(workspace_dependencies(&dependencies, "rfs"), ["rfs-common"]);
    assert_eq!(
        workspace_dependencies(&dependencies, "rfsd"),
        ["rfs-common"]
    );
    assert!(workspace_dependencies(&dependencies, "rfs-common").is_empty());
}

fn workspace_dependencies<'a>(
    packages: &'a BTreeMap<&str, Vec<&str>>,
    package: &str,
) -> Vec<&'a str> {
    packages[package]
        .iter()
        .copied()
        .filter(|dependency| packages.contains_key(dependency))
        .collect()
}
