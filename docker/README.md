# Runtime And API Images

Build two images from the same source revision or published package version:

| Target | Contents | Published tag pattern |
| --- | --- | --- |
| `runtime` (default) | Controller, worker, S3/Redis/Kafka/API/Iceberg adapters. No FastMCP, LanceDB, or embedding model. | `<version>`, `latest` |
| `api` | Shared runtime dependencies plus the `mcp` extra: REST/MCP, LanceDB, a local FastEmbed model, and version-matched documentation embeddings. | `<version>-api`, `latest-api` |

Use the runtime image for controller/worker/seed roles and the API image for
Topology. Both use the same mounted runtime YAML and existing Formula/Catalyst
services. LanceDB is embedded; it needs no service, credentials, or shared volume.
The API image can be larger because it carries ONNX Runtime and model assets.
The runtime has no FastMCP, LanceDB, FastEmbed, ONNX Runtime, LangChain, or Ollama
dependencies. Its remaining footprint is primarily Python/the OS base and the
PyArrow, DuckDB, and AWS/Kafka libraries needed for its storage/transport adapters.
Neither image bundles an Ollama server or a generative language model.

## Local Builds

From the repository root:

```bash
docker build --target runtime -f docker/oxidizer-lite.dockerfile -t oxidizer:local .
docker build --target api -f docker/oxidizer-lite.dockerfile -t oxidizer:local-api .
```

The named targets share their package/dependency base. The API build downloads
`BAAI/bge-small-en-v1.5` through FastEmbed, embeds bounded Markdown chunks, and
stores the model, LanceDB table, and compatibility manifest under
`/opt/oxidizer/search`. Only published documentation is indexed. Build-time
network access is required; serving loads the bundled model in offline mode.
Model details and license are available from the
[model publisher](https://huggingface.co/BAAI/bge-small-en-v1.5).

The final API image does not include build tools or a source checkout. Each
replica opens its own bundled index, including on a read-only root filesystem
with writable `/tmp` for Python/library temporary files. No persistent search
cache mount is needed. Changing docs, model, or indexing format requires a new
API image; changing a mounted workflow does not.

## Run

```bash
docker run --rm -p 127.0.0.1:8000:8000 \
	-v "$PWD/runtime.yml:/oxidizer/runtime.yml:ro" \
	oxidizer:local-api python -m app.api --config /oxidizer/runtime.yml

docker run --rm -v "$PWD/runtime.yml:/oxidizer/runtime.yml:ro" \
	oxidizer:local python -m app.controller --config /oxidizer/runtime.yml
```

Use `app.worker` for workers and mount custom modules as needed. The API's
default command is `python -m app.api`; its default YAML path is
`/oxidizer/controller+worker.yml`. The combined FastAPI app serves REST at
`/api/...` and MCP at `/mcp`. Keep REST/MCP behind trusted access control.

Check process liveness with `/api/health`, and semantic-search readiness with
`/api/docs/status` (200 ready, 503 unavailable). Missing/stale assets produce a
structured diagnostic, never a keyword-only fallback or runtime model download.
Other API features and `read_doc` remain available. See the
[MCP tool reference](../docs/reference/topology-api-and-tools.md#documentation-lookup).

## Release Builds

```bash
VERSION=1.2.3
docker build --target runtime -f docker/oxidizer-lite.release.dockerfile \
	--build-arg OXIDIZER_VERSION="$VERSION" -t "oxidizer:$VERSION" .
docker build --target api -f docker/oxidizer-lite.release.dockerfile \
	--build-arg OXIDIZER_VERSION="$VERSION" -t "oxidizer:$VERSION-api" .
```

Replace the example version with an existing PyPI release. The release
Dockerfile installs that exact `oxidizer-lite` version for both images and
builds the semantic assets from its bundled docs.

The active [release workflow](../.github/workflows/release.yml) runs when a
`v*` Git tag is pushed. After the Linux/macOS tests and package validation pass,
it publishes the package to PyPI, then builds and publishes `linux/amd64` and
`linux/arm64` variants of both
images to `ghcr.io/moos-engineering/oxidizer-lite`:

- Runtime: `<version>` and `latest`
- API: `<version>-api` and `latest-api`

The version is the pushed tag without its leading `v`. Editing files or pushing
a branch without a matching tag does not publish packages or images.

## Source Development

Without Docker, install the search dependencies and explicitly prepare assets:

```bash
python -m pip install -e '.[s3,redis,mcp]'
python -m oxidizer.utils.chisel --index-path /opt/oxidizer/search
```

Use a writable directory for the build. The default Topology location is
`/opt/oxidizer/search`; Python callers can construct `Chisel(index_path=...)`
for another location and assign it to `topology.documentation`. Building needs
network access; serving does not. Rebuild into a new empty directory after
updating docs or dependencies. The runtime package does not import semantic
dependencies unless Chisel initializes search or builds an index.
The `mcp` extra includes REST/MCP and search dependencies; there is no separate
`search` extra.
