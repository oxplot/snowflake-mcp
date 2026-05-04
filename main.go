package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/snowflakedb/gosnowflake"
)

const (
	serverVersion = "1.0.0"
	maxResultRows = 1000
	queryTimeout  = 5 * time.Minute

	maxOpenConnectionsPerTarget = 4
	maxIdleConnectionsPerTarget = 2
	connectionMaxIdleTime       = 5 * time.Minute
	connectionMaxLifetime       = 30 * time.Minute
)

var errExitSuccess = errors.New("exit successfully")

type queryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type queryResult struct {
	ColumnInfo   []queryColumn `json:"column_info"`
	Rows         [][]any       `json:"rows"`
	ReturnedRows int           `json:"returned_rows"`
	RowLimit     int           `json:"row_limit"`
	Truncated    bool          `json:"truncated"`
	Notice       string        `json:"notice,omitempty"`
}

type snowflakeTarget struct {
	Account   string
	Role      string
	Warehouse string
}

func newSnowflakeTarget(account, role, warehouse string) (snowflakeTarget, error) {
	target := snowflakeTarget{
		Account:   strings.TrimSpace(account),
		Role:      strings.TrimSpace(role),
		Warehouse: strings.TrimSpace(warehouse),
	}
	if target.Account == "" {
		return snowflakeTarget{}, fmt.Errorf("missing required account argument")
	}
	if target.Role == "" {
		return snowflakeTarget{}, fmt.Errorf("missing required role argument")
	}
	return target, nil
}

func (t snowflakeTarget) String() string {
	if t.Warehouse == "" {
		return fmt.Sprintf("account=%q role=%q warehouse=<default>", t.Account, t.Role)
	}
	return fmt.Sprintf("account=%q role=%q warehouse=%q", t.Account, t.Role, t.Warehouse)
}

type connectionManager struct {
	mu   sync.Mutex
	dbs  map[snowflakeTarget]*sqlx.DB
	open func(snowflakeTarget) (*sqlx.DB, error)
}

func newConnectionManager(open func(snowflakeTarget) (*sqlx.DB, error)) *connectionManager {
	if open == nil {
		open = openSnowflakeDB
	}
	return &connectionManager{
		dbs:  make(map[snowflakeTarget]*sqlx.DB),
		open: open,
	}
}

func (m *connectionManager) DB(target snowflakeTarget) (*sqlx.DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if db, ok := m.dbs[target]; ok {
		return db, nil
	}
	db, err := m.open(target)
	if err != nil {
		return nil, err
	}
	m.dbs[target] = db
	return db, nil
}

func (m *connectionManager) Close() error {
	m.mu.Lock()
	dbs := make([]*sqlx.DB, 0, len(m.dbs))
	for target, db := range m.dbs {
		dbs = append(dbs, db)
		delete(m.dbs, target)
	}
	m.mu.Unlock()

	var closeErr error
	for _, db := range dbs {
		closeErr = errors.Join(closeErr, db.Close())
	}
	return closeErr
}

func openSnowflakeDB(target snowflakeTarget) (*sqlx.DB, error) {
	sfconfig := gosnowflake.Config{
		Account:       target.Account,
		Role:          target.Role,
		Warehouse:     target.Warehouse,
		Authenticator: gosnowflake.AuthTypeExternalBrowser,
	}
	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, sfconfig)
	db := sqlx.NewDb(sql.OpenDB(connector), "snowflake")
	db.SetMaxOpenConns(maxOpenConnectionsPerTarget)
	db.SetMaxIdleConns(maxIdleConnectionsPerTarget)
	db.SetConnMaxIdleTime(connectionMaxIdleTime)
	db.SetConnMaxLifetime(connectionMaxLifetime)
	return db, nil
}

func run() error {
	if err := parseArgs(os.Args[1:]); err != nil {
		return err
	}

	connections := newConnectionManager(openSnowflakeDB)

	mcpServer := server.NewMCPServer("Snowflake", serverVersion)
	addQueryTool(mcpServer, connections)

	err := server.ServeStdio(mcpServer)
	if closeErr := connections.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("failed to close snowflake connections: %w", closeErr))
	}
	return err
}

func parseArgs(args []string) error {
	fs := flag.NewFlagSet("snowflake-mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	version := fs.Bool("version", false, "print version")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: snowflake-mcp [--version]\n\n")
		fmt.Fprintf(fs.Output(), "Runs an MCP server over stdio for Snowflake SQL queries.\n")
		fmt.Fprintf(fs.Output(), "Snowflake account, role, warehouse, and SQL are supplied by MCP query tool calls.\n\n")
		fmt.Fprintf(fs.Output(), "Example:\n")
		fmt.Fprintf(fs.Output(), "  codex mcp add snowflake -- go run github.com/oxplot/snowflake-mcp@4d60c1c44d6268a98beb0d35da73d7f4f100f5f3\n\n")
		fmt.Fprintf(fs.Output(), "Options:\n")
		fmt.Fprintf(fs.Output(), "  -h, --help   show help\n")
		fmt.Fprintf(fs.Output(), "  --version    print version\n")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return err
	}
	if *version {
		fmt.Fprintf(os.Stdout, "snowflake-mcp %s\n", serverVersion)
		return errExitSuccess
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected command-line argument %q", fs.Arg(0))
	}
	return nil
}

func addQueryTool(mcpServer *server.MCPServer, connections *connectionManager) {
	mcpServer.AddTool(mcp.NewTool(
		"query",
		mcp.WithDescription("Execute SQL against Snowflake using the requested account, role, and optional warehouse. Returns at most 1000 rows as structured content. This server does not block writes; Snowflake RBAC is the permission boundary."),
		mcp.WithString("account",
			mcp.Required(),
			mcp.Description("Snowflake account identifier to connect to, for example PPXXXXX-XXXXXXX."),
		),
		mcp.WithString("role",
			mcp.Required(),
			mcp.Description("Snowflake role to use for this query."),
		),
		mcp.WithString("warehouse",
			mcp.Description("Snowflake warehouse to use for this query. Pass a warehouse for deterministic execution; omit to leave Snowflake's session/default warehouse in effect."),
		),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SQL query to execute. Prefer SELECT and SHOW statements. Use full database.schema.table names when object context matters."),
		),
		mcp.WithOutputSchema[queryResult](),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Account   string `json:"account"`
			Role      string `json:"role"`
			Warehouse string `json:"warehouse"`
			Query     string `json:"query"`
		}
		if err := request.BindArguments(&args); err != nil {
			return mcp.NewToolResultErrorFromErr("failed to parse query arguments", err), nil
		}
		target, err := newSnowflakeTarget(args.Account, args.Role, args.Warehouse)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		query := strings.TrimSpace(args.Query)
		if query == "" {
			return mcp.NewToolResultError("missing required query argument"), nil
		}

		db, err := connections.DB(target)
		if err != nil {
			return mcp.NewToolResultErrorf("failed to initialize snowflake connection for %s: %v", target, err), nil
		}
		result, err := executeQuery(ctx, db, target, query)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return result, nil
	})
}

func executeQuery(ctx context.Context, db *sqlx.DB, target snowflakeTarget, query string) (*mcp.CallToolResult, error) {
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	conn, err := db.Connx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get snowflake connection for %s: %w", target, err)
	}
	defer conn.Close()

	if err := useSnowflakeTarget(ctx, conn, target); err != nil {
		return nil, err
	}

	rows, err := conn.QueryxContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query for %s: %w", target, err)
	}
	defer rows.Close()

	columnInfo := []queryColumn{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types for %s: %w", target, err)
	}
	for _, columnType := range columnTypes {
		columnInfo = append(columnInfo, queryColumn{
			Name: columnType.Name(),
			Type: columnType.DatabaseTypeName(),
		})
	}

	rowsSlice := [][]any{}
	truncated := false
	for rows.Next() {
		r, err := rows.SliceScan()
		if err != nil {
			return nil, fmt.Errorf("failed to scan row for %s: %w", target, err)
		}
		if len(rowsSlice) >= maxResultRows {
			truncated = true
			break
		}
		rowsSlice = append(rowsSlice, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows for %s: %w", target, err)
	}

	result := queryResult{
		ColumnInfo:   columnInfo,
		Rows:         rowsSlice,
		ReturnedRows: len(rowsSlice),
		RowLimit:     maxResultRows,
		Truncated:    truncated,
	}
	if truncated {
		result.Notice = fmt.Sprintf("Only first %d rows are shown", maxResultRows)
	}
	return newQueryToolResult(result)
}

func newQueryToolResult(result queryResult) (*mcp.CallToolResult, error) {
	b := bytes.NewBuffer(nil)
	jsonEnc := json.NewEncoder(b)
	jsonEnc.SetIndent("", " ")
	if err := jsonEnc.Encode(result); err != nil {
		return nil, fmt.Errorf("failed to marshal query result: %w", err)
	}
	return mcp.NewToolResultStructured(result, b.String()), nil
}

func useSnowflakeTarget(ctx context.Context, conn *sqlx.Conn, target snowflakeTarget) error {
	// Pooled Snowflake sessions are mutable, so reapply the target context before every query.
	if _, err := conn.ExecContext(ctx, "USE ROLE IDENTIFIER(?)", target.Role); err != nil {
		return fmt.Errorf("failed to use role for %s: %w", target, err)
	}
	if target.Warehouse == "" {
		return nil
	}
	if _, err := conn.ExecContext(ctx, "USE WAREHOUSE IDENTIFIER(?)", target.Warehouse); err != nil {
		return fmt.Errorf("failed to use warehouse for %s: %w", target, err)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		if errors.Is(err, flag.ErrHelp) || errors.Is(err, errExitSuccess) {
			return
		}
		fmt.Fprintf(os.Stderr, "snowflake-mcp: error: %v\n", err)
		os.Exit(1)
	}
}
