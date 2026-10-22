# Releases

[Release-plz](https://release-plz.dev/docs/github/quickstart) prepares version and
changelog PRs automatically on `main`. Reviewing and merging a release PR is the
manual approval to publish: its merge starts the **Publish** workflow.
Regular PRs, unmerged closed PRs, and tag pushes do not publish artifacts.

The internal crates share the workspace version. The server keeps its own
version. Server-only changes bump the server; internal changes also bump the
server when it depends on them. Nothing is published to crates.io.

## Publish

1. Review and merge the release-plz PR. Use Conventional Commit titles for code
   changes so release-plz can determine the next versions.
2. Merging the PR into `main` automatically starts **Publish**, which runs nextest
   and doctests when that PR changes `.rs` files. The PR must come from a
   `release-plz-*` branch in this repository.
3. Release-plz creates the pending `server-v<version>` and
   `internals-v<version>` tags. The internal tag is a version baseline only.
4. Server releases are published as Docker images, compiled on native Linux
   AMD64 and ARM64 runners from the released tag.

The Docker image is `ghcr.io/<owner>/pgtest`, using the repository's current owner,
with `sha-<12-character-commit>` (recommended) and `v<server-version>` tags for
the same multi-platform image.
It supports `linux/amd64` and `linux/arm64`.

## Manual Docker nightly

Open **Actions → Docker nightly → Run workflow**, select the branch to build,
and click **Run workflow**. The workflow must be present on the default branch
to appear in the UI and on the selected branch to run there.

The workflow runs the existing tests when the selected commit changes `.rs`
files, then publishes
`linux/amd64` and `linux/arm64` images to `ghcr.io/<owner>/pgtest`, using the
repository's current owner. It publishes these tags:

- `nightly`: the most recently published manual nightly from any branch.
- `nightly-<branch>`: the latest nightly for that branch; Docker metadata replaces
  unsupported tag characters, such as `/`, with `-`.
- `nightly-sha-<12-character-commit>`: the image for the selected commit.

Versioned release tags are unchanged. This workflow has no automatic schedule.
To trigger it from the CLI:

```sh
gh workflow run docker-nightly.yml --ref your-branch
```

## Docker build pipeline

Nightly and versioned Docker releases share `docker-publish.yml`. It resolves the
selected source to a commit, compiles the server on native Blacksmith AMD64 and
ARM64 runners with the pinned Rust toolchain and musl, and verifies static linkage.
Each runner packages and smoke-tests its scratch image before uploading a tar
archive of the runtime filesystem. Tar preserves executable permissions and the
writable `/tmp` directory across artifact transfer.

After both builds succeed, a packaging job uses `Dockerfile.cd` to copy the
prepared files into one image per architecture and publish a multi-platform
image. Packaging requires no QEMU or Docker build cache. Cargo dependencies and
compiled dependency artifacts use `Swatinem/rust-cache`, with separate musl
release cache keys per architecture. Build artifacts expire after one day.

The existing `Dockerfile` still compiles from source for local builds on macOS:

```sh
docker build -t pgtest:local .
```

`Dockerfile.cd` is for prepared artifacts only. Its build context contains
`amd64/` and `arm64/`, each with `usr/local/bin/pgtest-server` (mode `0755`) and
`tmp/` (mode `1777`). `scripts/docker-build.sh build` prepares those files;
`PGTEST_OUTPUT_DIR` selects the output directory and defaults to `/out` for the
local Docker build. Linker flags remain in `.cargo/config.toml`.

## Validation and retries

Workspace tests and doctests run only for `.rs` changes, including additions,
deletions and renames. PR checks compare the full PR diff; pushes to `main`
compare the complete pushed range. A separate `changed` job uses
`dorny/paths-filter` and skips the entire test job for other changes, allowing
releases to continue without installing test tools or compiling the workspace. Manifest, lockfile, documentation and workflow changes alone skip tests.

For failures, use GitHub's **Re-run failed jobs** on the affected workflow.
Re-running the entire Publish workflow skips versions already tagged by
release-plz; it is not a publication retry.
