# snowflake-mcp

An MCP server for querying Snowflake over stdio.

Authentication uses Snowflake external browser auth only, so the server does not need to store Snowflake credentials on disk.

**Warning:** this server does not try to block write queries. Your real protection is the Snowflake role you give it. Use a dedicated least-privilege role, and prefer a read-only role unless you intentionally want write access.

## What You Need

- A Snowflake account identifier, for example `PPXXXXX-XXXXXXX`
- A Snowflake role the MCP client should use
- Optionally, a warehouse name if your role does not have a usable default warehouse
- A downloaded `snowflake-mcp` binary from this repo's GitHub Releases page

## Install The Binary

`goreleaser` currently publishes:

- `.tar.gz` archives for macOS and Linux
- `.zip` archives for Windows
- `amd64` and `arm64` builds for each supported OS

Download the newest release archive that matches your OS and CPU from GitHub Releases, then extract the `snowflake-mcp` binary to a stable absolute path.

Current archive naming:

- macOS Apple Silicon: `snowflake-mcp_<version>_darwin_arm64.tar.gz`
- macOS Intel: `snowflake-mcp_<version>_darwin_amd64.tar.gz`
- Linux ARM64: `snowflake-mcp_<version>_linux_arm64.tar.gz`
- Linux Intel: `snowflake-mcp_<version>_linux_amd64.tar.gz`
- Windows Intel: `snowflake-mcp_<version>_windows_amd64.zip`
- Windows ARM: `snowflake-mcp_<version>_windows_arm64.zip`

### macOS

Choose one of:

- Apple Silicon: `snowflake-mcp_<version>_darwin_arm64.tar.gz`
- Intel: `snowflake-mcp_<version>_darwin_amd64.tar.gz`

Install to a stable path:

```sh
mkdir -p ~/.local/bin
tar -xzf ~/Downloads/snowflake-mcp_<version>_darwin_arm64.tar.gz -C ~/.local/bin snowflake-mcp
chmod +x ~/.local/bin/snowflake-mcp
```

### Linux

Choose one of:

- ARM64: `snowflake-mcp_<version>_linux_arm64.tar.gz`
- x86_64: `snowflake-mcp_<version>_linux_amd64.tar.gz`

Install to a stable path:

```sh
mkdir -p ~/.local/bin
tar -xzf ~/Downloads/snowflake-mcp_<version>_linux_amd64.tar.gz -C ~/.local/bin snowflake-mcp
chmod +x ~/.local/bin/snowflake-mcp
```

### Windows

Choose one of:

- ARM64: `snowflake-mcp_<version>_windows_arm64.zip`
- x64: `snowflake-mcp_<version>_windows_amd64.zip`

Extract to a stable path with PowerShell:

```powershell
New-Item -ItemType Directory -Force "$HOME\AppData\Local\Programs\snowflake-mcp" | Out-Null
Expand-Archive `
  -Path "$HOME\Downloads\snowflake-mcp_<version>_windows_amd64.zip" `
  -DestinationPath "$HOME\AppData\Local\Programs\snowflake-mcp" `
  -Force
```

Use a stable path such as `~/.local/bin/snowflake-mcp` on macOS or Linux, or `%USERPROFILE%\AppData\Local\Programs\snowflake-mcp\snowflake-mcp.exe` on Windows. Do not point Claude Desktop or Codex CLI at a versioned download directory, or every upgrade will break the config.

On Windows, use the full path to `snowflake-mcp.exe` in the client config.

If you want to verify the download, compare the archive checksum with `checksums.txt` from the same release.

## Runtime Flags

The binary accepts:

- `-account=...` required
- `-role=...` required
- `-warehouse=...` optional

Example:

```sh
/Users/you/.local/bin/snowflake-mcp \
  -account=PPXXXXX-XXXXXXX \
  -role=REPORTER \
  -warehouse=ANALYTICS
```

On first use, Snowflake browser auth will open a browser window so you can sign in interactively.

## Claude Desktop

Claude Desktop's newest local-MCP experience is Desktop Extensions, but this repository currently ships standalone binaries rather than `.mcpb` bundles. For this project, the practical setup is still a local stdio server entry that launches the binary directly.

### macOS

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "snowflake": {
      "command": "/Users/you/.local/bin/snowflake-mcp",
      "args": [
        "-account=PPXXXXX-XXXXXXX",
        "-role=REPORTER",
        "-warehouse=ANALYTICS"
      ]
    }
  }
}
```

### Windows

Edit `%APPDATA%\Claude\claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "snowflake": {
      "command": "C:\\Users\\you\\AppData\\Local\\Programs\\snowflake-mcp\\snowflake-mcp.exe",
      "args": [
        "-account=PPXXXXX-XXXXXXX",
        "-role=REPORTER",
        "-warehouse=ANALYTICS"
      ]
    }
  }
}
```

### Linux

Claude Desktop is currently documented by Anthropic for macOS and Windows, not Linux.

Notes for Claude Desktop:

- Use the full absolute path to the binary
- Omit `-warehouse=...` if Snowflake can resolve a default warehouse for that role
- Restart Claude Desktop after saving the config if the server does not appear immediately

Once connected, ask Claude to inspect resources such as databases, schemas, tables, and views, or call the `query` tool with fully qualified table names.

## Codex CLI

The current Codex CLI has built-in MCP management commands. The simplest setup is to register the binary with `codex mcp add`.

### macOS or Linux

```sh
codex mcp add snowflake -- \
  /Users/you/.local/bin/snowflake-mcp \
  -account=PPXXXXX-XXXXXXX \
  -role=REPORTER \
  -warehouse=ANALYTICS
```

### Windows

```powershell
codex mcp add snowflake -- `
  C:\Users\you\AppData\Local\Programs\snowflake-mcp\snowflake-mcp.exe `
  -account=PPXXXXX-XXXXXXX `
  -role=REPORTER `
  -warehouse=ANALYTICS
```

Then confirm it is registered on any OS:

```sh
codex mcp list
```

If you prefer to edit the Codex config file directly instead of using `codex mcp add`, add:

```toml
[mcp_servers.snowflake]
command = "/Users/you/.local/bin/snowflake-mcp"
args = [
  "-account=PPXXXXX-XXXXXXX",
  "-role=REPORTER",
  "-warehouse=ANALYTICS",
]
```

## Troubleshooting

- If Claude Desktop or Codex cannot start the server, check that the binary path is absolute and executable
- If macOS blocks the binary the first time, allow it in System Settings > Privacy & Security, then retry
- If Snowflake login succeeds but queries fail, the role likely lacks `USAGE`, `SELECT`, or warehouse access
- If you see unexpected write capability, fix the Snowflake role; this server intentionally relies on Snowflake RBAC rather than adding its own SQL permission layer
