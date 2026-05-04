# snowflake-mcp

Local MCP server for querying Snowflake.

Authentication uses Snowflake external browser auth only, so the server does not
need to store Snowflake credentials on disk.

**WARNING:** the MCP client chooses the Snowflake account, role, warehouse, and
SQL for each query. This server does not block write queries or allowlist
Snowflake targets. Your protection is the Snowflake role you give it. Use
dedicated least-privilege roles, and prefer read-only roles unless you
intentionally want write access.

## What You Need

- Go 1.26 or newer
- The Snowflake account identifiers you want the MCP client to query, for
  example `PPXXXXX-XXXXXXX`
- The Snowflake roles the MCP client should use
- Optionally, warehouse names if the authenticated user does not have a usable
  default warehouse for the selected role

## Installation

Register the MCP server with Codex by running the Go module directly at a pinned
commit:

```sh
codex mcp add snowflake -- \
  go run github.com/oxplot/snowflake-mcp@28ddd2c99d11df6a8b37359c8a9c7d59b6c10375
```

The first startup fetches and builds the pinned module through the standard Go
toolchain. Keep the hash pinned for repeatable MCP behavior, and update it
intentionally when you want Codex to use a newer revision.

## Usage

You need to ask your agent to run a query using the snowflake tool. You must
provide your agent with:

- the Snowflake account identifier, for example `PPXXXXX-XXXXXXX`
- the Snowflake role to use, for example `REPORTER`
- optionally, the Snowflake warehouse to use, for example `ANALYTICS`
- the SQL query to run (or have the agent generate it).

### Session State

Before each query, the server runs `USE ROLE` and, when `warehouse` is provided,
`USE WAREHOUSE`. It does not reset database, schema, session parameters,
variables, or temporary objects. Queries should be self-contained and use fully
qualified object names when object context matters.

Inspect metadata through the `query` tool with SQL such as `SHOW DATABASES`,
`SHOW SCHEMAS`, or `INFORMATION_SCHEMA` queries.

## Troubleshooting

- If Codex cannot start the server, check that `go version` works in the same
  shell environment Codex uses
- If Snowflake login succeeds but queries fail, the role likely lacks `USAGE`,
  `SELECT`, or warehouse access for the target supplied to that query call
- If you see unexpected write capability, fix the Snowflake role; this server
  intentionally relies on Snowflake RBAC rather than adding its own SQL
  permission layer
