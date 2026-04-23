# Repository Bootstrap

This guide bootstraps one provider-backed repository against Shardline without changing
the normal Git workflow.

If you want the minimal setup sequence first, start with
[Provider Setup Guide](PROVIDER_QUICKSTART.md).
For a direct providerless backend without a forge connector, start with
[Providerless Direct Xet Backend](DEPLOYMENT.md#providerless-direct-xet-backend).

You only need to decide:

- which repository scope the connector will mint tokens for
- which native Xet client or filter process will speak to the CAS

## 1. Prepare Shardline

Start with:

- a reachable Shardline base URL
- a configured token signing key
- a provider catalog that includes the repository
- a trusted provider-side service that can call `POST /v1/providers/{provider}/tokens`

## 2. Register The Repository

Each repository entry defines:

- provider kind
- owner or namespace
- repository name
- visibility
- default revision
- clone URL
- subjects allowed to read
- subjects allowed to write

Example entry:

```json
{
  "kind": "github",
  "integration_subject": "github-app",
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
```

## 3. Decide The Client Path

Use the native Xet path when the client already supports the Xet CAS protocol.

## 4. Configure The Repository

### Native Xet Path

Point the client or filter configuration at the Shardline CAS base URL and make sure the
provider-side connector mints repository-scoped bearer tokens.

## 5. Verify Token Issuance

The provider-side service should exchange an authorization result for a CAS token:

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

The response token must be repository-scoped and short-lived.

## 6. Verify Upload And Download

For native Xet clients:

- upload a file
- update a small region of the file
- confirm the second write deduplicates unchanged chunks
- fetch the latest version and an older version

## 7. Cut Over

Recommended order:

1. enable repository-scoped provider issuance
2. point the repository client or filter at the native CAS base URL
3. validate upload, sparse update, and historical fetch behavior
4. roll the same configuration to every environment that serves the repository
