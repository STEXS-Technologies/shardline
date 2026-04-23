# Provider Setup Guide

This is the shortest documented path to a provider-backed Shardline deployment with an
existing GitHub, GitLab, or Gitea repository and stock `git-lfs` plus `git-xet`.

It is still a real setup process: you need a provider catalog, a provider-facing bridge,
and explicit webhook delivery.

The goal is to keep normal Git usage and swap the large-object backend underneath it.
The provider catalog is a real file that you create and point the server at with
`SHARDLINE_PROVIDER_CONFIG_FILE`.

## What You Need

Before touching the repository, have these ready:

- a reachable Shardline base URL
- a token-signing key
- a provider bootstrap key
- a provider catalog entry for the repository
- a provider-side bridge or connector that can:
  - exchange provider authorization for a Shardline CAS token
  - forward provider webhook deliveries to Shardline
  - keep the repository's Git LFS URL pointing at the normal forge-facing endpoint

Shardline is the CAS backend. Your existing forge stays the forge.
That means there are two distinct setup layers:

1. Shardline itself:
   - provider catalog
   - signing key
   - bootstrap key
   - object store and metadata store
2. the forge-facing bridge:
   - checks user access on the forge
   - asks Shardline for a short-lived CAS token
   - hands that token to the Xet client path
   - forwards forge webhooks to Shardline

## 0. Create the Real Files

For a local source checkout, create a working directory:

```bash
mkdir -p .shardline
printf %s 'dev-signing-key' > .shardline/token-signing-key
printf %s 'dev-provider-bootstrap-key' > .shardline/provider-api-key
chmod 600 .shardline/token-signing-key .shardline/provider-api-key
```

That `0600` mode is correct for a local source-checkout run.
If you bind-mount the same files into the non-root Docker image, the mounted files must
also be readable by the container user.
For local Docker examples, use either a secret mount with matching UID or GID, or relax
the bind-mounted file mode explicitly for that development setup.

Now create the provider catalog at a real path on disk:

```bash
cat > .shardline/providers.json <<'JSON'
{
  "providers": [
    {
      "kind": "github",
      "integration_subject": "github-app",
      "webhook_secret": "replace-me",
      "repositories": [
        {
          "owner": "team",
          "name": "assets",
          "visibility": "private",
          "default_revision": "main",
          "clone_url": "https://github.example/team/assets.git",
          "read_subjects": ["github-user-1"],
          "write_subjects": ["github-user-1"]
        }
      ]
    }
  ]
}
JSON
```

For GitLab or Gitea, change only the provider kind and repository identity:

- GitLab: `"kind": "gitlab"`
- Gitea: `"kind": "gitea"`

Where this file lives depends on the deployment mode:

- local binary or `cargo run`:
  - any host path, for example `<repo>/.shardline/providers.json`
- Docker:
  - any host path, mounted into the container, for example
    `/absolute/host/path/.shardline/providers.json -> /etc/shardline/providers.json`
- Kubernetes:
  - the `providers.json` key inside the provider catalog Secret, mounted by the API pods
    at `/etc/shardline/providers/providers.json`

## 1. Register the Repository

Add the repository to the provider catalog.

If you already have the repository on the provider, pull the values from the provider or
from the cloned remote instead of typing them by hand.

GitHub from `gh`:

```bash
gh repo view <owner>/<repo> \
  --json owner,name,visibility,defaultBranchRef,url \
  --jq '{
    owner: .owner.login,
    name,
    visibility: (.visibility | ascii_downcase),
    default_revision: .defaultBranchRef.name,
    clone_url: (.url + ".git")
  }'
```

That gives you the exact fields you need for:

- `owner`
- `name`
- `visibility`
- `default_revision`
- `clone_url`

Gitea from `tea` plus the repository remote:

```bash
tea repos ls --login <login-name> --owner <owner> --limit 1 \
  --fields owner,name,ssh,url,permission,type \
  --output json

git ls-remote --symref https://gitea.example/<owner>/<repo>.git HEAD
```

That gives you:

- `owner`
- `name`
- a canonical repository URL to turn into `clone_url`
- `default_revision` from `HEAD`

For the live `stexs/stexs` repository this resolved to:

- `owner=stexs`
- `name=stexs`
- `clone_url=https://gitea.stexs.net/stexs/stexs.git`
- `default_revision=main`

Gitea from an existing clone:

```bash
git config --get remote.origin.url
git symbolic-ref --short refs/remotes/origin/HEAD | sed 's#^origin/##'
```

That gives you:

- `clone_url` from `remote.origin.url`
- `default_revision` from `origin/HEAD`

For a remote such as `https://gitea.example/team/assets.git`, the catalog values are:

- `owner=team`
- `name=assets`

If `tea` is authenticated against the same Gitea instance, prefer the `tea repos ls`
path above and use the cloned remote only to confirm `default_revision`.

Minimal shape:

```json
{
  "providers": [
    {
      "kind": "github",
      "integration_subject": "github-app",
      "webhook_secret": "replace-me",
      "repositories": [
        {
          "owner": "team",
          "name": "assets",
          "visibility": "private",
          "default_revision": "main",
          "clone_url": "https://github.example/team/assets.git",
          "read_subjects": ["user-1"],
          "write_subjects": ["user-1"]
        }
      ]
    }
  ]
}
```

Provider-specific mapping:

- GitHub: `kind=github`, `owner=<org-or-user>`, `name=<repo>`
- GitLab: `kind=gitlab`, `owner=<group-or-namespace>`, `name=<project>`
- Gitea: `kind=gitea`, `owner=<org-or-user>`, `name=<repo>`

## 2. Start Shardline

The bundled compose file is a plain local CAS profile.
It does not automatically mount provider configuration or bootstrap keys for you.

For a provider-aware source-checkout setup, start Shardline directly so the config paths
are explicit:

```bash
SHARDLINE_BIND_ADDR=127.0.0.1:18080 \
SHARDLINE_SERVER_ROLE=all \
SHARDLINE_PUBLIC_BASE_URL=http://127.0.0.1:18080 \
SHARDLINE_ROOT_DIR="$PWD/.shardline/data" \
SHARDLINE_OBJECT_STORAGE_ADAPTER=local \
SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory \
SHARDLINE_TOKEN_SIGNING_KEY_FILE="$PWD/.shardline/token-signing-key" \
SHARDLINE_PROVIDER_CONFIG_FILE="$PWD/.shardline/providers.json" \
SHARDLINE_PROVIDER_API_KEY_FILE="$PWD/.shardline/provider-api-key" \
cargo run -p shardline --bin shardline -- serve
```

If you want the same thing in Docker instead of `cargo run`, mount the files into the
container and point the environment variables at the mounted paths.
The `/data/shardline` path below is inside the container; the host still keeps state in
the current project's `.shardline/data` directory.
The bind-mounted key files must be readable by the non-root container user.

```bash
chmod 644 .shardline/token-signing-key .shardline/provider-api-key

docker run --rm -it \
  -p 18080:8080 \
  -e SHARDLINE_BIND_ADDR=0.0.0.0:8080 \
  -e SHARDLINE_SERVER_ROLE=all \
  -e SHARDLINE_PUBLIC_BASE_URL=http://127.0.0.1:18080 \
  -e SHARDLINE_ROOT_DIR=/data/shardline \
  -e SHARDLINE_OBJECT_STORAGE_ADAPTER=local \
  -e SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER=memory \
  -e SHARDLINE_TOKEN_SIGNING_KEY_FILE=/run/secrets/shardline/token-signing-key \
  -e SHARDLINE_PROVIDER_CONFIG_FILE=/etc/shardline/providers.json \
  -e SHARDLINE_PROVIDER_API_KEY_FILE=/run/secrets/shardline/provider-api-key \
  -v "$PWD/.shardline/data:/data/shardline" \
  -v "$PWD/.shardline/token-signing-key:/run/secrets/shardline/token-signing-key:ro" \
  -v "$PWD/.shardline/provider-api-key:/run/secrets/shardline/provider-api-key:ro" \
  -v "$PWD/.shardline/providers.json:/etc/shardline/providers.json:ro" \
  shardline:local serve
```

For Kubernetes, the provider catalog is already modeled as a Secret in the production
package:

- [provider-catalog-secret.template.yaml](k8s/production-scaled/provider-catalog-secret.template.yaml)

and the API deployment mounts it at:

- `/etc/shardline/providers/providers.json`

For the scaled production profile, use the Kubernetes package described in
[Kubernetes Manifests](k8s/README.md).

At minimum, a running provider-aware deployment needs:

- `SHARDLINE_PUBLIC_BASE_URL`
- `SHARDLINE_TOKEN_SIGNING_KEY_FILE`
- `SHARDLINE_PROVIDER_CONFIG_FILE`
- `SHARDLINE_PROVIDER_API_KEY_FILE`

If you use S3 and Postgres in production, also set the corresponding storage and index
configuration from [Deployment](DEPLOYMENT.md).

## 3. Wire the Provider Bridge

This is the part people usually skip when they only show the JSON. Without a
provider-facing bridge, users do not automatically get CAS tokens just because the
catalog file exists.

The provider-facing bridge needs two routes into Shardline:

- token issuance:
  - `POST /v1/providers/{provider}/tokens`
- webhook ingestion:
  - `POST /v1/providers/{provider}/webhooks`

The bridge should:

1. authenticate the user against the forge
2. resolve repository access through the forge
3. request a short-lived repository-scoped CAS token from Shardline
4. hand that token to the native Xet client path
5. forward provider webhooks to Shardline with the configured webhook secret

Example token request:

```http
POST /v1/providers/github/tokens
X-Shardline-Provider-Key: <bootstrap-key>
Content-Type: application/json

{
  "subject": "github-user-1",
  "owner": "team",
  "repo": "assets",
  "revision": "refs/heads/main",
  "scope": "Write"
}
```

You can verify that the provider catalog is actually loaded and the bootstrap key is
actually wired before building the bridge by sending that request manually:

```bash
curl \
  -X POST \
  -H 'x-shardline-provider-key: dev-provider-bootstrap-key' \
  -H 'content-type: application/json' \
  http://127.0.0.1:18080/v1/providers/github/tokens \
  -d '{
    "subject": "github-user-1",
    "owner": "team",
    "repo": "assets",
    "revision": "refs/heads/main",
    "scope": "Write"
  }'
```

If that returns a token response, the catalog file location, bootstrap key file, and
provider registration are all wired correctly.

## 4. Prepare the Repository

On the client machine:

```bash
git lfs install --local
git xet install --local --path .
git xet track "*.bin"
```

Then configure the repository to keep using the normal forge-facing LFS endpoint:

```bash
git config lfs.url https://forge.example/team/assets.git/info/lfs
git config lfs.locksverify false
```

That keeps the user-visible workflow familiar:

- `git add`
- `git commit`
- `git push`
- `git clone`
- `git fetch`
- `git pull`

`git-xet` handles the native Xet CAS path underneath, while the forge-facing LFS entry
point remains the place where the repository already expects large-object traffic.

In a real forge-backed deployment, the bridge is the component that serves or fronts
that LFS URL and injects the repository-scoped CAS token into the Xet client path.

## 5. Verify the Happy Path

Run this once per repository rollout:

```bash
python - <<'PY'
from pathlib import Path
Path("asset.bin").write_bytes(bytes((i * 17 + 31) % 256 for i in range(512 * 1024)))
PY

git add .gitattributes asset.bin
git commit -m "add xet tracked asset"
git push
```

Then change a small region and push again:

```bash
python - <<'PY'
from pathlib import Path
data = bytearray(Path("asset.bin").read_bytes())
data[128 * 1024:160 * 1024] = b'Z' * (32 * 1024)
Path("asset.bin").write_bytes(data)
PY

git add asset.bin
git commit -m "update asset"
git push
```

Verification target:

- the second push uploads only the changed chunks
- clone, fetch, pull, and checkout still work as normal Git commands
- the latest file reconstructs correctly
- older commits still reconstruct correctly

From a source checkout, there is also a live smoke path that creates one new private
GitHub repository and one new private Gitea repository, then runs the full bridge test
against both of them:

```bash
scripts/shardline/live-provider-bridge-smoke.sh
```

Inputs:

- GitHub authentication from `gh auth`
- Gitea authentication from `tea`
- optional overrides:
  - `SHARDLINE_LIVE_BRIDGE_REPO_NAME`
  - `SHARDLINE_LIVE_GITHUB_OWNER`
  - `SHARDLINE_LIVE_GITEA_OWNER`
  - `SHARDLINE_LIVE_GITEA_BASE_URL`
  - `SHARDLINE_LIVE_GITEA_LOGIN_NAME`

That smoke path proves the bridge behavior against real private repositories instead of
only local fixtures:

- Git push still goes to the provider
- Git LFS batch traffic goes through the bridge
- `git-xet` uploads through Shardline native CAS
- a small file mutation stores only new chunk data for the changed region
- clone and checkout reconstruct both the latest and previous file versions correctly

## 6. Webhooks

Configure repository lifecycle webhooks from the forge to Shardline:

- GitHub: repository events and push events
- GitLab: project/repository lifecycle events and push events
- Gitea: repository lifecycle events and push events

The actual destination is the provider webhook route on Shardline:

- GitHub:
  - `POST /v1/providers/github/webhooks`
- GitLab:
  - `POST /v1/providers/gitlab/webhooks`
- Gitea:
  - `POST /v1/providers/gitea/webhooks`

Shardline uses those to:

- reconcile repository state
- process rename and deletion safely
- keep retention and cleanup behavior correct

## 7. Rollout Order

Use this order for a real repository:

1. add the repository to the provider catalog
2. verify provider token issuance
3. configure provider webhooks
4. install `git-lfs` and `git-xet`
5. set the repository LFS URL to the normal forge-facing endpoint
6. track one test asset with `git xet track`
7. push an initial version
8. push a small mutation
9. verify clone, fetch, pull, and historical checkout

## When You Need More Detail

Use these documents after the setup guide:

- [Repository Bootstrap](REPOSITORY_BOOTSTRAP.md)
- [Client Configuration](CLIENT_CONFIGURATION.md)
- [Provider Adapters](PROVIDER_ADAPTERS.md)
- [Deployment](DEPLOYMENT.md)
- [Operations](OPERATIONS.md)
