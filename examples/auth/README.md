# GitHub OAuth Quickstart

Run Topology with GitHub OAuth for both REST and MCP. Like the Redis
quickstart, this example uses Redis for streams and RustFS for workflow
storage. The `auth_hello` workflow has one node that writes a greeting to
a stream. There are no Parquet or Iceberg prerequisites.

## Set up GitHub

1. [Register a GitHub OAuth app](https://github.com/settings/applications/new)
   with homepage `http://localhost:8028` and callback
   `http://localhost:8028/mcp/auth/callback`. Use `localhost` consistently
   when connecting to this example.
2. In [runtime.yml](runtime.yml), replace the sample `client_id` and the
   `allowed_github_user_ids` entry with your app's client ID and your numeric
   GitHub user ID (`gh api user --jq .id` if you're signed into GitHub CLI).
3. Generate a client secret and have your secure configuration loader supply
   `topology.oauth.client_secret` in the runtime configuration passed to the API.
   The checked-in field is intentionally empty; don't commit a real secret.
   Topology does not resolve environment variables or secret references for this field.

From `examples/auth`, build the images from the current checkout and start:

```bash
docker compose build controller api
docker compose up -d
docker compose logs seed
```

Wait for `Seeded auth_hello`. The API listens only on loopback at port 8028,
so the regular quickstart can keep using 8027.

## Verify authentication

```bash
curl -i http://localhost:8028/api/health   # 200: public process health
curl -i http://localhost:8028/api/formulas # 401: bearer token required
```

In VS Code/Copilot, add `http://localhost:8028/mcp` as a Streamable HTTP MCP
server. Complete GitHub browser sign-in, then call `list_formulas`. It should
list `auth_hello`. Sign-in does not require pasting a token into VS Code.

If your OAuth client gives you access to the **Topology-issued access token**,
you can also use it with REST. A GitHub access token or app client secret
won't work. Set `TOPOLOGY_TOKEN` securely, then run:

```bash
curl --fail-with-body http://localhost:8028/api/formulas \
  -H "Authorization: Bearer $TOPOLOGY_TOKEN"

curl --fail-with-body http://localhost:8028/api/invocations \
  -H "Authorization: Bearer $TOPOLOGY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"formula_id":"auth_hello"}'
```

Use the returned `run_id` to inspect the run with `get_formula_state` in MCP
or `GET /api/formulas/auth_hello/runs/<run_id>` with the same bearer token.

Stop just this example with `docker compose down --volumes` from `examples/auth`.
See the [Topology auth reference](../../docs/reference/topology-api-and-tools.md#optional-github-oauth)
for the runtime settings.
