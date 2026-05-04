# snowflake-mcp

An MCP server for querying Snowflake over stdio.

Authentication uses Snowflake external browser auth only, so the server does not need to store Snowflake credentials on disk.

**Warning:** this server does not try to block write queries. Your real protection is the Snowflake role you give it. Use a dedicated least-privilege role, and prefer a read-only role unless you intentionally want write access.

## What You Need

- Go 1.26 or newer
- The Snowflake account identifiers you want the MCP client to query, for example `PPXXXXX-XXXXXXX`
- The Snowflake roles the MCP client should use
- Optionally, warehouse names if your roles do not have usable default warehouses

## Codex CLI

Register the MCP server with Codex by running the Go module directly at a pinned commit:

```sh
codex mcp add snowflake -- \
  go run github.com/oxplot/snowflake-mcp@b61b3603806cafeeb9f42b1654a616b6ae4497eb
```

Then confirm it is registered:

```sh
codex mcp list
```

Equivalent Codex config:

```toml
[mcp_servers.snowflake]
command = "go"
args = [
  "run",
  "github.com/oxplot/snowflake-mcp@b61b3603806cafeeb9f42b1654a616b6ae4497eb",
]
```

The first startup fetches and builds the pinned module through the standard Go toolchain. Keep the hash pinned for repeatable MCP behavior, and update it intentionally when you want Codex to use a newer revision.

## Runtime Behavior

The server exposes one MCP tool named `query`, and every tool call supplies the Snowflake target:

- `account` required
- `role` required
- `warehouse` optional
- `query` required

Example:

```json
{
  "account": "PPXXXXX-XXXXXXX",
  "role": "REPORTER",
  "warehouse": "ANALYTICS",
  "query": "SELECT * FROM MY_DATABASE.MY_SCHEMA.MY_TABLE LIMIT 10"
}
```

The server keeps a separate Snowflake connection pool for each `account`/`role`/`warehouse` combination and creates each pool lazily on first use. Before each query, it reapplies the requested role and, when provided, warehouse on the checked-out Snowflake connection. On first use of a new combination, Snowflake browser auth will open a browser window so you can sign in interactively.

Inspect metadata through the `query` tool with SQL such as `SHOW DATABASES`, `SHOW SCHEMAS`, or `INFORMATION_SCHEMA` queries.

## Troubleshooting

- If Codex cannot start the server, check that `go version` works in the same shell environment Codex uses
- If Snowflake login succeeds but queries fail, the role likely lacks `USAGE`, `SELECT`, or warehouse access for the target supplied to that query call
- If you see unexpected write capability, fix the Snowflake role; this server intentionally relies on Snowflake RBAC rather than adding its own SQL permission layer
