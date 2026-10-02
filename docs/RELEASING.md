# Releasing rustyq

Releases are cut by pushing a `vX.Y.Z` tag. `.github/workflows/release.yml`
then builds wheels and an sdist, publishes to PyPI and crates.io, pushes the
image to GHCR, and creates the GitHub release. Publishing jobs use GitHub
environments, so they pause until a maintainer approves them.

## One-time setup

1. **PyPI trusted publisher** (the project does not exist yet, so use a
   pending publisher). On pypi.org: Account settings -> Publishing -> "Add a
   new pending publisher" with PyPI project name `rustyq`, Owner `Bunty9`,
   Repository `rustyq`, Workflow `release.yml`, Environment `pypi`.

2. **crates.io.** Trusted publishing can only be configured on a crate that
   already exists, so the first release uses an API token:
   - Create a crates.io API token scoped to `publish-new` and
     `publish-update` for `rustyq-*`, then store it:
     `gh secret set CARGO_REGISTRY_TOKEN --repo Bunty9/rustyq`
   - Approve the `crates-io` deployment when the release runs.
   - After the first release, for each of `rustyq-core`, `rustyq-client`,
     `rustyq-server`, `rustyq-worker`: crates.io -> the crate -> Settings ->
     Trusted Publishing -> Add, with Repository owner `Bunty9`, Repository
     `rustyq`, Workflow `release.yml`, Environment `crates-io`.
   - Then remove the fallback: `gh secret delete CARGO_REGISTRY_TOKEN --repo
     Bunty9/rustyq`, and revoke the token on crates.io.

3. **GitHub environments.** Create `pypi` and `crates-io` (Settings ->
   Environments) with the maintainer as a required reviewer. Publishing jobs
   wait until approved in the Actions UI.

4. **GHCR.** The first push creates `ghcr.io/bunty9/rustyq` as private. In
   the package settings, make it public and link it to the repository.

## Per release

1. Bump `version` in `[workspace.package]` in the root `Cargo.toml` and in the
   `version = "..."` of the `rustyq-core` path dependencies in
   `crates/server/Cargo.toml` and `crates/worker/Cargo.toml`; run
   `cargo check --workspace` to refresh `Cargo.lock`.
2. Add a `## [X.Y.Z] - YYYY-MM-DD` section to `CHANGELOG.md` (the GitHub
   release notes are taken from it).
3. Commit, tag and push:
   ```bash
   git commit -am "release: vX.Y.Z"
   git tag -a vX.Y.Z -m "rustyq X.Y.Z"
   git push origin main vX.Y.Z
   ```
4. Approve the `pypi` and `crates-io` deployments in the Actions run.

The workflow fails early if the tag does not match the workspace version, and
the crates job skips any crate whose version is already on crates.io, so a
failed run can be re-run safely.

## Dry run

Actions -> release -> "Run workflow" on a branch (not a tag). Everything
builds, but the version-vs-tag check is skipped and no publish, image push or
release job runs: those only run for a pushed `v*` tag, never for a manual
dispatch. To check packaging locally:

```bash
cargo package -p rustyq-core -p rustyq-server -p rustyq-worker -p rustyq-client
maturin build --release -m crates/pybind/Cargo.toml
```
