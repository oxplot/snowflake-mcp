An MCP for querying Snowflake. External browser auth is the only one
supported in order to avoid storing Snowflake credentials on disk.

**WARNING: No attempt has been made to disallow writes. Your only
defence against a malicious/misbehaving LLM is the permissions you grant
to the Snowflake account.**

## Use with Claude Code CLI

```sh
claude mcp add-json snowflake '{
  "command": "go",
  "args": [
    "run",
    "github.com/oxplot/snowflake-mcp@latest",
    "-account=PPXXXXX-XXXXXXX",
    "-role=reporter"
  ]
}'

```
