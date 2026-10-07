# Releases

[Release-plz](https://release-plz.dev/docs/github/quickstart) prepares version and
changelog PRs automatically on `main`. Reviewing and merging a release PR is the
manual approval to publish: its merge starts the **Publish** workflow.
Regular PRs, unmerged closed PRs, and tag pushes do not publish artifacts.

The internal crates share the workspace version. The CLI and server keep their
own versions. App-only changes bump that app; internal changes also bump the apps
that depend on them. Nothing is published to crates.io.

## Publish

1. Review and merge the release-plz PR. Use Conventional Commit titles for code
   changes so release-plz can determine the next versions.
2. Merging the PR into `main` automatically starts **Publish**, which runs nextest
   and doctests against that PR's merge commit. The PR must come from a
   `release-plz-*` branch in this repository.
3. Release-plz creates the pending `cli-v<version>`, `server-v<version>`, and
   `internals-v<version>` tags. The internal tag is a version baseline only.
4. Changed apps are published: Docker compiles on native Linux AMD64 and ARM64 runners;
   the CLI starts the separate **Release** workflow (`cli-release.yml`) generated
   by cargo-dist.
   Check that workflow too: **Publish** finishes once the CLI run is started.

The Docker image is `ghcr.io/<owner>/pgtest`, using the repository's current owner,
with `sha-<12-character-commit>` (recommended) and `v<server-version>` tags for
the same multi-platform image.
It supports `linux/amd64` and `linux/arm64`.

[Cargo-dist](https://axodotdev.github.io/cargo-dist/book/installers/homebrew.html)
builds CLI archives for Linux x86_64/ARM64 and Apple Silicon macOS, publishes
GitHub downloads and a shell installer, and updates Homebrew:

```sh
brew install Afsoon/tap/pgtest
```

The CLI embeds its version and full source commit at build time. Both publishers
build the tags created by release-plz, so later changes to `main` are excluded.
Prereleases do not update Homebrew.

## Manual Docker nightly

Open **Actions → Docker nightly → Run workflow**, select the branch to build,
and click **Run workflow**. The workflow must be present on the default branch
to appear in the UI and on the selected branch to run there.

The workflow runs the existing tests against the selected commit, then publishes
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

## macOS signing and notarization

Developer ID signing and notarization are not part of the release plan or
requirements for version 1.0. The CLI workflow requires no Apple signing or
notarization credentials. Keep the toolchain's default ad-hoc signing intact; it does not
identify the publisher or provide notarization.

Downloaded macOS binaries may require explicit user approval. The root
[README](README.md#macos-releases) documents this limitation.

## Validation and retries

Run **Release** with its default `dry-run` tag to build the CLI artifacts
without publishing. PRs also check cargo-dist's release plan.

For failures, use GitHub's **Re-run failed jobs** on the affected workflow.
Re-running the entire Publish workflow skips versions already tagged by
release-plz; it is not a publication retry. If the CLI dispatch itself needs to
be repeated, use its existing tag as both the workflow ref and tag input:

```sh
gh workflow run cli-release.yml --ref cli-v0.1.0 -f tag=cli-v0.1.0
```

Edit `dist-workspace.toml` and `.github/dist/build-setup.yml`, then run
`dist generate` to update the generated CLI workflow. Do not edit
`.github/workflows/cli-release.yml` directly.
