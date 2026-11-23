# Run the GitHub OAuth demo once

1. In your GitHub OAuth app, set the homepage to `http://localhost:8028` and the
   callback to `http://localhost:8028/mcp/auth/callback`.
2. In [runtime.yml](./runtime.yml), fill in `topology.oauth.client_id`,
   `client_secret`, and `allowed_github_user_ids` with your app credentials and
   your numeric GitHub user ID (`gh api user --jq .id`). Do not commit the secret.
3. From the repository root, start the demo:

   ```sh
   cd examples/auth
   docker compose build controller api
   docker compose up -d
   curl --fail http://localhost:8028/api/health
   ```

   Health is public; this confirms startup, not sign-in. If it is not ready yet,
   check `docker compose logs seed api` and retry.

## Sign in, invoke, and check status

Run this locally with `uv`. Complete GitHub sign-in when the browser opens.
The client uses that same sign-in to invoke the workflow and poll its REST
status without printing or copying a token.

```sh
uv run --no-project --with 'fastmcp>=4,<5' python - <<'PY'
import asyncio

import httpx2
from fastmcp import Client
from fastmcp.client.auth import OAuth

BASE = "http://localhost:8028"
MCP = f"{BASE}/mcp/"


async def main():
    auth = OAuth(MCP)
    async with Client(MCP, auth=auth), httpx2.AsyncClient(auth=auth) as rest:
        response = await rest.post(f"{BASE}/api/invocations", json={"formula_id": "auth_hello"})
        response.raise_for_status()
        run_id = response.json()["run_id"]
        print(f"Run: {run_id}", flush=True)

        for _ in range(30):
            response = await rest.get(f"{BASE}/api/formulas/auth_hello/runs/{run_id}")
            if response.status_code != 404:  # The controller may not have created the run yet.
                response.raise_for_status()
                status = response.json()["state"]["status"]
                print(status, flush=True)
                if status == "success":
                    return
                if status in {"failed", "cancelled", "timed_out", "skipped"}:
                    raise SystemExit(f"Run ended with status: {status}")
            await asyncio.sleep(1)
        raise SystemExit("Still waiting after 30 seconds; check the controller and worker logs.")


asyncio.run(main())
PY
```

Once you see `success`, run `docker compose down --volumes` from `examples/auth`
and remove your local credentials from [runtime.yml](./runtime.yml).
