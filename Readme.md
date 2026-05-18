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

- `snowflake-mcp` installed on your `PATH`
- The Snowflake account identifiers you want the MCP client to query, for
  example `PPXXXXX-XXXXXXX`
- The Snowflake roles the MCP client should use
- Optionally, warehouse names if the authenticated user does not have a usable
  default warehouse for the selected role
- Go 1.26 or newer only if you use the `go install` fallback

## Installation

### macOS

Install from the Homebrew tap in this repository:

```sh
brew tap oxplot/snowflake-mcp https://github.com/oxplot/snowflake-mcp
brew install snowflake-mcp
codex mcp add snowflake -- snowflake-mcp
```

### Linux and Windows

Download the archive for your OS and CPU from the latest GitHub release:

https://github.com/oxplot/snowflake-mcp/releases/latest

Release archives are named by OS and CPU:

- `snowflake-mcp_linux_x86_64.tar.gz`
- `snowflake-mcp_linux_arm64.tar.gz`
- `snowflake-mcp_windows_x86_64.zip`
- `snowflake-mcp_windows_arm64.zip`

Extract the archive, put `snowflake-mcp` or `snowflake-mcp.exe` somewhere on
your `PATH`, then register it with Codex:

```sh
codex mcp add snowflake -- snowflake-mcp
```

### Last Resort: Go Install

If no release binary is available for your system, install from source with Go:

```sh
go install github.com/oxplot/snowflake-mcp@latest
codex mcp add snowflake -- snowflake-mcp
```

This uses the standard Go toolchain and requires Go 1.26 or newer.

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

- If Codex cannot start the server, check that `snowflake-mcp --version` works
  in the same shell environment Codex uses
- If you installed with `go install`, also check that `go version` works in that
  shell environment
- If Snowflake login succeeds but queries fail, the role likely lacks `USAGE`,
  `SELECT`, or warehouse access for the target supplied to that query call
- If you see unexpected write capability, fix the Snowflake role; this server
  intentionally relies on Snowflake RBAC rather than adding its own SQL
  permission layer
