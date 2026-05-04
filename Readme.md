# snowflake-mcp

Local MCP server for querying Snowflake.

Authentication uses Snowflake external browser auth only, so the server does not need to store Snowflake credentials on disk.

**WARNING:** the MCP client chooses the Snowflake account, role, warehouse, and SQL for each query. This server does not block write queries or allowlist Snowflake targets. Your protection is the Snowflake role you give it. Use dedicated least-privilege roles, and prefer read-only roles unless you intentionally want write access.

## What You Need

- Go 1.26 or newer
- The Snowflake account identifiers you want the MCP client to query, for example `PPXXXXX-XXXXXXX`
- The Snowflake roles the MCP client should use
- Optionally, warehouse names if the authenticated user does not have a usable default warehouse for the selected role

## Codex CLI

Register the MCP server with Codex by running the Go module directly at a pinned commit:

```sh
codex mcp add snowflake -- \
  go run github.com/oxplot/snowflake-mcp@4d60c1c44d6268a98beb0d35da73d7f4f100f5f3
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
  "github.com/oxplot/snowflake-mcp@4d60c1c44d6268a98beb0d35da73d7f4f100f5f3",
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

Results are returned as structured MCP content with a compact text summary. `column_info` contains column name/type pairs, `rows` contains row arrays, `returned_rows` reports the number of rows returned, `row_limit` reports the server row cap, `result_bytes` reports the encoded response size, `result_byte_limit` reports the response cap, `cell_byte_limit` reports the per-cell cap, and `truncated` reports whether more data was available. When `truncated` is true, `truncated_reason` is one of `row_limit`, `result_size`, or `cell_size`.

Each query has a 5-minute timeout. The server returns at most 1,000 rows and may read one additional row to determine whether the row limit was exceeded. Responses are capped at 5 MiB, and individual cells are capped at 1 MiB. Use SQL `LIMIT`, filters, projections, or aggregates for larger result sets.

The server keeps a separate Snowflake connection pool for each `account`/`role`/`warehouse` combination and creates each pool lazily on first use. It keeps at most 32 distinct target pools open at once. Each target pool allows up to 4 open Snowflake connections and 2 idle connections. On first use of a new combination, Snowflake browser auth will open a browser window so you can sign in interactively.

### Session State

Before each query, the server runs `USE ROLE` and, when `warehouse` is provided, `USE WAREHOUSE`. It does not reset database, schema, session parameters, variables, or temporary objects. Queries should be self-contained and use fully qualified object names when object context matters.

Inspect metadata through the `query` tool with SQL such as `SHOW DATABASES`, `SHOW SCHEMAS`, or `INFORMATION_SCHEMA` queries.

## Troubleshooting

- If Codex cannot start the server, check that `go version` works in the same shell environment Codex uses
- If Snowflake login succeeds but queries fail, the role likely lacks `USAGE`, `SELECT`, or warehouse access for the target supplied to that query call
- If you see unexpected write capability, fix the Snowflake role; this server intentionally relies on Snowflake RBAC rather than adding its own SQL permission layer
