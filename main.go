package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/snowflakedb/gosnowflake"
)

const (
	serverVersion      = "1.0.0"
	maxResultRows      = 1000
	maxResultBytes     = 5 * 1024 * 1024
	maxCellBytes       = 1 * 1024 * 1024
	queryTimeout       = 5 * time.Minute
	queryTimeoutLabel  = "5m"
	resultBudgetBuffer = 16 * 1024

	maxOpenConnectionsPerTarget = 4
	maxIdleConnectionsPerTarget = 2
	maxTargetPools              = 32
	connectionMaxIdleTime       = 5 * time.Minute
	connectionMaxLifetime       = 30 * time.Minute

	stdioWorkerPoolSize = 4
	stdioQueueSize      = 16
)

type parseAction int

const (
	parseActionServe parseAction = iota
	parseActionExit
)

const (
	truncatedByRowLimit   = "row_limit"
	truncatedByResultSize = "result_size"
	truncatedByCellSize   = "cell_size"
)

// queryColumn describes one column in the returned row arrays.
type queryColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// queryResult is the structured MCP response for the query tool.
//
// Rows are arrays in column order. The server returns at most maxResultRows and
// also enforces byte budgets so large result sets cannot overwhelm stdio clients.
type queryResult struct {
	ColumnInfo      []queryColumn `json:"column_info"`
	Rows            [][]any       `json:"rows"`
	ReturnedRows    int           `json:"returned_rows"`
	RowLimit        int           `json:"row_limit"`
	ResultBytes     int           `json:"result_bytes"`
	ResultByteLimit int           `json:"result_byte_limit"`
	CellByteLimit   int           `json:"cell_byte_limit"`
	Truncated       bool          `json:"truncated"`
	TruncatedReason string        `json:"truncated_reason,omitempty"`
	Notice          string        `json:"notice,omitempty"`
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
	mu         sync.Mutex
	dbs        map[snowflakeTarget]*sqlx.DB
	open       func(snowflakeTarget) (*sqlx.DB, error)
	maxTargets int
}

func newConnectionManager(open func(snowflakeTarget) (*sqlx.DB, error)) *connectionManager {
	return newConnectionManagerWithLimit(open, maxTargetPools)
}

func newConnectionManagerWithLimit(open func(snowflakeTarget) (*sqlx.DB, error), maxTargets int) *connectionManager {
	if open == nil {
		open = openSnowflakeDB
	}
	if maxTargets <= 0 {
		maxTargets = maxTargetPools
	}
	return &connectionManager{
		dbs:        make(map[snowflakeTarget]*sqlx.DB),
		open:       open,
		maxTargets: maxTargets,
	}
}

func (m *connectionManager) DB(target snowflakeTarget) (*sqlx.DB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if db, ok := m.dbs[target]; ok {
		return db, nil
	}
	if len(m.dbs) >= m.maxTargets {
		return nil, fmt.Errorf("too many Snowflake targets are open: maximum is %d distinct account/role/warehouse pools", m.maxTargets)
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
	action, err := parseArgs(os.Args[1:])
	if err != nil {
		return err
	}
	if action == parseActionExit {
		return nil
	}

	connections := newConnectionManager(openSnowflakeDB)

	mcpServer := server.NewMCPServer("Snowflake", serverVersion, server.WithRecovery())
	addQueryTool(mcpServer, connections)

	err = server.ServeStdio(
		mcpServer,
		server.WithWorkerPoolSize(stdioWorkerPoolSize),
		server.WithQueueSize(stdioQueueSize),
	)
	if closeErr := connections.Close(); closeErr != nil {
		return errors.Join(err, fmt.Errorf("failed to close snowflake connections: %w", closeErr))
	}
	return err
}

func parseArgs(args []string) (parseAction, error) {
	if hasHelpFlag(args) {
		printUsage(os.Stdout)
		return parseActionExit, nil
	}

	fs := flag.NewFlagSet("snowflake-mcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.Bool("version", false, "print version")
	fs.Usage = func() {
		printUsage(fs.Output())
	}
	if err := fs.Parse(args); err != nil {
		return parseActionExit, formatFlagParseError(args, err)
	}
	if *version {
		fmt.Fprintln(os.Stdout, versionString())
		return parseActionExit, nil
	}
	if fs.NArg() != 0 {
		return parseActionExit, fmt.Errorf("unexpected command-line argument %q\nThis command starts an MCP stdio server and does not accept positional arguments.\nRun snowflake-mcp --help for setup", fs.Arg(0))
	}
	return parseActionServe, nil
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" || arg == "-help" {
			return true
		}
	}
	return false
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: snowflake-mcp [--version]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Runs an MCP server over stdio for Snowflake SQL queries.")
	fmt.Fprintln(w, "Snowflake account, role, warehouse, and SQL are supplied by MCP query tool calls.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Safety:")
	fmt.Fprintln(w, "  The MCP client chooses the Snowflake account, role, warehouse, and SQL for each query.")
	fmt.Fprintln(w, "  This server does not block writes; restrict Snowflake role permissions.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Example:")
	fmt.Fprintln(w, `  codex mcp add snowflake "$(command -v snowflake-mcp)`)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Options:")
	fmt.Fprintln(w, "  -h, --help   show help")
	fmt.Fprintln(w, "  --version    print version")
}

func formatFlagParseError(args []string, err error) error {
	if strings.Contains(err.Error(), "flag provided but not defined") {
		return fmt.Errorf("unknown option %s\nAccount, role, warehouse, and SQL are MCP query tool arguments, not process flags.\nRun snowflake-mcp --help for setup", unknownFlag(args))
	}
	return fmt.Errorf("%v\nRun snowflake-mcp --help for setup", err)
}

func unknownFlag(args []string) string {
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			continue
		}
		name, _, _ := strings.Cut(arg, "=")
		if name == "-h" || name == "--help" || name == "-help" || name == "--version" || name == "-version" {
			continue
		}
		if strings.HasPrefix(name, "--") {
			return name
		}
		return "-" + strings.TrimLeft(name, "-")
	}
	return "<unknown>"
}

func versionString() string {
	revision := "unknown"
	modified := false
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		revision += "+modified"
	}
	return fmt.Sprintf("snowflake-mcp %s commit=%s", serverVersion, revision)
}

func addQueryTool(mcpServer *server.MCPServer, connections *connectionManager) {
	mcpServer.AddTool(mcp.NewTool(
		"query",
		mcp.WithDescription("Execute SQL against Snowflake using the requested account, role, and optional warehouse. Returns at most 1000 rows as structured content. This server does not block writes or allowlist targets; Snowflake RBAC is the permission boundary."),
		mcp.WithTitleAnnotation("Snowflake SQL Query"),
		mcp.WithReadOnlyHintAnnotation(false),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(false),
		mcp.WithOpenWorldHintAnnotation(true),
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
		return nil, snowflakeQueryError(ctx, target, "failed to get snowflake connection", err)
	}
	defer conn.Close()

	if err := useSnowflakeTarget(ctx, conn, target); err != nil {
		if queryTimedOut(ctx, err) {
			return nil, queryTimeoutError(target)
		}
		return nil, err
	}

	rows, err := conn.QueryxContext(ctx, query)
	if err != nil {
		return nil, snowflakeQueryError(ctx, target, "failed to execute query", err)
	}
	defer rows.Close()

	columnInfo := []queryColumn{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, snowflakeQueryError(ctx, target, "failed to get column types", err)
	}
	for _, columnType := range columnTypes {
		columnInfo = append(columnInfo, queryColumn{
			Name: columnType.Name(),
			Type: columnType.DatabaseTypeName(),
		})
	}

	rowsSlice := [][]any{}
	rowsJSONBytes := 0
	truncatedReason := ""
	for rows.Next() {
		r, err := rows.SliceScan()
		if err != nil {
			return nil, snowflakeQueryError(ctx, target, "failed to scan row", err)
		}
		if len(rowsSlice) >= maxResultRows {
			truncatedReason = truncatedByRowLimit
			break
		}
		oversizedCell, err := rowHasOversizedCell(r)
		if err != nil {
			return nil, fmt.Errorf("failed to measure row for %s: %w", target, err)
		}
		if oversizedCell {
			truncatedReason = truncatedByCellSize
			break
		}
		rowBytes, err := rowJSONSize(r)
		if err != nil {
			return nil, fmt.Errorf("failed to measure row for %s: %w", target, err)
		}
		nextRowsJSONBytes := rowsJSONBytes + rowBytes
		if len(rowsSlice) > 0 {
			nextRowsJSONBytes++
		}
		if nextRowsJSONBytes+resultBudgetBuffer > maxResultBytes {
			truncatedReason = truncatedByResultSize
			break
		}
		rowsSlice = append(rowsSlice, r)
		rowsJSONBytes = nextRowsJSONBytes
	}
	if err := rows.Err(); err != nil {
		return nil, snowflakeQueryError(ctx, target, "failed to read rows", err)
	}

	result := queryResult{
		ColumnInfo:      columnInfo,
		Rows:            rowsSlice,
		ReturnedRows:    len(rowsSlice),
		RowLimit:        maxResultRows,
		ResultByteLimit: maxResultBytes,
		CellByteLimit:   maxCellBytes,
		Truncated:       truncatedReason != "",
		TruncatedReason: truncatedReason,
	}
	if result.Truncated {
		result.Notice = truncationNotice(truncatedReason)
	}
	if err := fitQueryResultToBudget(&result); err != nil {
		return nil, err
	}
	return newQueryToolResult(result)
}

func newQueryToolResult(result queryResult) (*mcp.CallToolResult, error) {
	summary := fmt.Sprintf(
		"Returned %d rows (row_limit=%d, result_bytes=%d, result_byte_limit=%d, truncated=%t)",
		result.ReturnedRows,
		result.RowLimit,
		result.ResultBytes,
		result.ResultByteLimit,
		result.Truncated,
	)
	if result.TruncatedReason != "" {
		summary += fmt.Sprintf(" reason=%s", result.TruncatedReason)
	}
	if result.Notice != "" {
		summary += ". " + result.Notice
	}
	return mcp.NewToolResultStructured(result, summary), nil
}

func fitQueryResultToBudget(result *queryResult) error {
	result.ReturnedRows = len(result.Rows)
	result.RowLimit = maxResultRows
	result.ResultByteLimit = maxResultBytes
	result.CellByteLimit = maxCellBytes

	if err := setQueryResultBytes(result); err != nil {
		return err
	}
	for result.ResultBytes > maxResultBytes && len(result.Rows) > 0 {
		result.Rows = result.Rows[:len(result.Rows)-1]
		result.ReturnedRows = len(result.Rows)
		result.Truncated = true
		result.TruncatedReason = truncatedByResultSize
		result.Notice = truncationNotice(truncatedByResultSize)
		if err := setQueryResultBytes(result); err != nil {
			return err
		}
	}
	if result.ResultBytes > maxResultBytes {
		return fmt.Errorf("query result metadata exceeded %d byte response limit", maxResultBytes)
	}
	return nil
}

func setQueryResultBytes(result *queryResult) error {
	for i := 0; i < 8; i++ {
		b, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("failed to marshal query result: %w", err)
		}
		if result.ResultBytes == len(b) {
			return nil
		}
		result.ResultBytes = len(b)
	}
	return nil
}

func rowHasOversizedCell(row []any) (bool, error) {
	for _, cell := range row {
		size, err := cellByteSize(cell)
		if err != nil {
			return false, err
		}
		if size > maxCellBytes {
			return true, nil
		}
	}
	return false, nil
}

func cellByteSize(cell any) (int, error) {
	switch v := cell.(type) {
	case nil:
		return 0, nil
	case string:
		return len(v), nil
	case []byte:
		return len(v), nil
	case json.RawMessage:
		return len(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return 0, err
		}
		return len(b), nil
	}
}

func rowJSONSize(row []any) (int, error) {
	b, err := json.Marshal(row)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func truncationNotice(reason string) string {
	switch reason {
	case truncatedByRowLimit:
		return fmt.Sprintf("Only first %d rows are shown", maxResultRows)
	case truncatedByResultSize:
		return fmt.Sprintf("Result stopped before the %d byte response limit; narrow the query, select fewer columns, or aggregate results", maxResultBytes)
	case truncatedByCellSize:
		return fmt.Sprintf("Result stopped because a cell exceeded the %d byte limit; select smaller values or summarize large fields", maxCellBytes)
	default:
		return "Result was truncated"
	}
}

func snowflakeQueryError(ctx context.Context, target snowflakeTarget, action string, err error) error {
	if queryTimedOut(ctx, err) {
		return queryTimeoutError(target)
	}
	return fmt.Errorf("%s for %s: %w", action, target, err)
}

func queryTimedOut(ctx context.Context, err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func queryTimeoutError(target snowflakeTarget) error {
	return fmt.Errorf("query exceeded %s timeout for %s; narrow the query with LIMIT, filters, or aggregates", queryTimeoutLabel, target)
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
		fmt.Fprintf(os.Stderr, "snowflake-mcp: error: %v\n", err)
		os.Exit(1)
	}
}
