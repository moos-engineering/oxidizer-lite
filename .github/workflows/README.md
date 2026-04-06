# GitHub Actions Workflows

## Release Workflow

The `release.yml` workflow builds and publishes the package to PyPI when a version tag is pushed.

### Triggering a Release

```bash
# Tag the release
git tag v0.1.0

# Push the tag
git push origin v0.1.0
```

### What Happens

1. **Test** — Runs pytest across Python 3.10, 3.11, 3.12, and 3.13
2. **Build** — Creates wheel and source distribution
3. **Publish** — Uploads to PyPI using trusted publishing (OIDC)

### PyPI Trusted Publishing Setup

Configure trusted publishing on PyPI before your first release:

1. Go to https://pypi.org/manage/account/publishing/
2. Add a new pending publisher:
   - **Owner**: `moos-engineering`
   - **Repository**: `oxidizer-lite`
   - **Workflow**: `release.yml`
   - **Environment**: `release`

### Version Tagging Convention

Tags must start with `v` followed by semver:
- `v0.1.0` — Initial release
- `v0.1.1` — Patch release
- `v0.2.0` — Minor release
- `v1.0.0` — Major release

Remember to update `version` in `pyproject.toml` before tagging.
