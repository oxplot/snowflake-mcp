# Snowflake MCP - Claude Guidelines

## Commands
- Build: `go build`
- Run: `go run main.go -account=<account> -role=<role> -warehouse=<warehouse>` 
- Test: `go test ./...`
- Test single package: `go test ./path/to/package`
- Test with verbose output: `go test -v ./...`
- Lint: `go vet ./...`
- Format code: `go fmt ./...`

## Code Style Guidelines
- **Imports**: Group imports by standard library, external, then project imports
- **Error Handling**: Return errors with context using `fmt.Errorf("Failed to...: %w", err)`
- **Naming**: Use CamelCase for exported functions/types, camelCase for unexported
- **Types**: Prefer strong types over generic ones like `interface{}`/`any` when possible
- **Functions**: Keep functions focused and under ~100 lines
- **Comments**: Document exported functions and types with meaningful comments
- **Regex**: Compile regexp patterns once and reuse
- **SQL**: Use parameterized queries to prevent SQL injection

## Project Structure
This is a Snowflake MCP (Machine Communication Protocol) client that connects to Snowflake and provides database exploration capabilities via MCP protocol.