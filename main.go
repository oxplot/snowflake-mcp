package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/snowflakedb/gosnowflake"
)

const maxResultRows = 1000

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
	return sqlx.NewDb(sql.OpenDB(connector), "snowflake").Unsafe(), nil
}

func run() error {
	if err := parseArgs(os.Args[1:]); err != nil {
		return err
	}

	connections := newConnectionManager(openSnowflakeDB)

	mcpServer := server.NewMCPServer("Snowflake", "1.0.0")
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
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: snowflake-mcp\n\n")
		fmt.Fprintf(fs.Output(), "Snowflake account, role, and warehouse are supplied to each query tool call, not as process flags.\n")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return fmt.Errorf("%w; account, role, and warehouse are query tool arguments, not process flags", err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected command-line argument %q; account, role, and warehouse are query tool arguments", fs.Arg(0))
	}
	return nil
}

func addQueryTool(mcpServer *server.MCPServer, connections *connectionManager) {
	mcpServer.AddTool(mcp.NewTool(
		"query",
		mcp.WithDescription("Execute a Snowflake SQL query against the requested account, role, and warehouse."),
		mcp.WithString("account",
			mcp.Required(),
			mcp.Description("Snowflake account identifier to connect to, for example PPXXXXX-XXXXXXX."),
		),
		mcp.WithString("role",
			mcp.Required(),
			mcp.Description("Snowflake role to use for this query."),
		),
		mcp.WithString("warehouse",
			mcp.Description("Snowflake warehouse to use for this query. Omit to use the role's default warehouse."),
		),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SQL query to execute. You must use full database.schema.table names when referencing tables."),
		),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args struct {
			Account   string `json:"account"`
			Role      string `json:"role"`
			Warehouse string `json:"warehouse"`
			Query     string `json:"query"`
		}
		if err := request.BindArguments(&args); err != nil {
			return nil, fmt.Errorf("failed to parse query arguments: %w", err)
		}
		target, err := newSnowflakeTarget(args.Account, args.Role, args.Warehouse)
		if err != nil {
			return nil, err
		}
		query := strings.TrimSpace(args.Query)
		if query == "" {
			return nil, fmt.Errorf("missing required query argument")
		}

		db, err := connections.DB(target)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize snowflake connection for %s: %w", target, err)
		}
		return executeQuery(ctx, db, target, query)
	})
}

func executeQuery(ctx context.Context, db *sqlx.DB, target snowflakeTarget, query string) (*mcp.CallToolResult, error) {
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
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnInfo := []map[string]any{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}
	for _, columnType := range columnTypes {
		columnInfo = append(columnInfo, map[string]any{
			"name": columnType.Name(),
			"type": columnType.DatabaseTypeName(),
		})
	}

	rowsSlice := [][]any{}
	for rows.Next() {
		r, err := rows.SliceScan()
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		rowsSlice = append(rowsSlice, r)
		if len(rowsSlice) >= maxResultRows {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	result := map[string]any{
		"column_info": columnInfo,
		"rows":        rowsSlice,
		"notice":      fmt.Sprintf("Only first %d rows are shown", maxResultRows),
	}
	b := bytes.NewBuffer(nil)
	jsonEnc := json.NewEncoder(b)
	jsonEnc.SetIndent("", " ")
	if err := jsonEnc.Encode(result); err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.TextContent{
				Type: "text",
				Text: b.String(),
			},
		},
	}, nil
}

func useSnowflakeTarget(ctx context.Context, conn *sqlx.Conn, target snowflakeTarget) error {
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
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatalf("Error: %v", err)
	}
}
