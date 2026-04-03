use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const WEB_BUILD_HINT: &str = "Build Web UI assets with `make build-web` or `cd web && dx build --release` before building hardata.";

fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = dst.join(entry.file_name());

        if file_type.is_dir() {
            fs::create_dir_all(&target)?;
            copy_dir_all(&entry.path(), &target)?;
        } else if file_type.is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), target)?;
        }
    }

    Ok(())
}

fn first_existing_dir(candidates: &[PathBuf]) -> Option<PathBuf> {
    candidates.iter().find(|path| path.is_dir()).cloned()
}

fn dir_contains_extension(dir: &Path, extension: &str) -> io::Result<bool> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;

        if file_type.is_dir() && dir_contains_extension(&path, extension)? {
            return Ok(true);
        }

        if file_type.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some(extension) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn ensure_web_assets_ready(out_dir: &Path) {
    let index_path = out_dir.join("index.html");
    let assets_dir = out_dir.join("assets");
    let style_path = assets_dir.join("style.css");

    assert!(
        index_path.is_file(),
        "Missing Web UI asset '{}'. {}",
        index_path.display(),
        WEB_BUILD_HINT
    );
    assert!(
        assets_dir.is_dir(),
        "Missing Web UI assets directory '{}'. {}",
        assets_dir.display(),
        WEB_BUILD_HINT
    );
    assert!(
        style_path.is_file(),
        "Missing Web UI stylesheet '{}'. {}",
        style_path.display(),
        WEB_BUILD_HINT
    );
    assert!(
        dir_contains_extension(&assets_dir, "js").unwrap(),
        "Missing bundled Web UI JavaScript under '{}'. {}",
        assets_dir.display(),
        WEB_BUILD_HINT
    );
    assert!(
        dir_contains_extension(&assets_dir, "wasm").unwrap(),
        "Missing bundled Web UI WASM under '{}'. {}",
        assets_dir.display(),
        WEB_BUILD_HINT
    );
}

fn prepare_web_assets(manifest_dir: &Path, out_dir: &Path) {
    let dist_dir = manifest_dir.join("web/dist");
    let dx_public_dir = manifest_dir.join("web/target/dx/hardata-web/release/web/public");
    let source_dir =
        first_existing_dir(&[dist_dir.clone(), dx_public_dir.clone()]).unwrap_or_else(|| {
            panic!(
                "Web UI assets not found. Expected '{}' or '{}'. {}",
                dist_dir.display(),
                dx_public_dir.display(),
                WEB_BUILD_HINT
            )
        });

    copy_dir_all(&source_dir, out_dir).unwrap_or_else(|error| {
        panic!(
            "Failed to copy Web UI assets from '{}' to '{}': {}",
            source_dir.display(),
            out_dir.display(),
            error
        )
    });

    let extra_assets_dir = manifest_dir.join("web/assets");
    if extra_assets_dir.is_dir() {
        let target_assets_dir = out_dir.join("assets");
        fs::create_dir_all(&target_assets_dir).unwrap();
        copy_dir_all(&extra_assets_dir, &target_assets_dir).unwrap_or_else(|error| {
            panic!(
                "Failed to copy extra Web UI assets from '{}' to '{}': {}",
                extra_assets_dir.display(),
                target_assets_dir.display(),
                error
            )
        });
    }

    // 构建阶段必须验证关键资源齐全，避免产出默认即损坏的二进制。
    ensure_web_assets_ready(out_dir);
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap()).join("hardata-web-dist");

    println!("cargo:rerun-if-changed=web");

    if out_dir.exists() {
        fs::remove_dir_all(&out_dir).unwrap();
    }
    fs::create_dir_all(&out_dir).unwrap();

    prepare_web_assets(&manifest_dir, &out_dir);
}
