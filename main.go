package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/snowflakedb/gosnowflake"
)

func run() error {
	var (
		snowflakeAccount   = flag.String("account", "", "Snowflake account name")
		snowflakeRole      = flag.String("role", "", "Snowflake role name")
		snowflakeWarehouse = flag.String("warehouse", "", "Snowflake warehouse name")
	)
	flag.Parse()
	if *snowflakeAccount == "" || *snowflakeRole == "" {
		return fmt.Errorf("Please provide account and role")
	}

	// Setup connection to snowflake using browser auth

	sfconfig := gosnowflake.Config{
		Account:       *snowflakeAccount,
		Role:          *snowflakeRole,
		Warehouse:     *snowflakeWarehouse,
		Authenticator: gosnowflake.AuthTypeExternalBrowser,
	}
	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, sfconfig)
	db := sqlx.NewDb(sql.OpenDB(connector), "snowflake").Unsafe()

	// Create MCP server

	mcpServer := server.NewMCPServer("Snowflake", "1.0.0")

	// Add a query tool.
	mcpServer.AddTool(mcp.NewTool(
		"query",
		mcp.WithDescription("Execute a SQL query."),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SQL query to execute.  You must use full database.schema.table when referencing tables."),
		),
	), func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {

		query, _ := request.Params.Arguments["query"].(string)
		const maxResultRows = 1000

		// Execute the query.
		rows, err := db.QueryxContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("Failed to execute query: %v", err)
		}
		defer rows.Close()

		// Get column details.
		columnInfo := []map[string]any{}
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, fmt.Errorf("Failed to get column types: %v", err)
		}
		for _, columnType := range columnTypes {
			columnInfo = append(columnInfo, map[string]any{
				"name": columnType.Name(),
				"type": columnType.DatabaseTypeName(),
			})
		}

		// Fetch the rows.
		rowsSlice := [][]any{}
		for rows.Next() {
			r := []any{}
			r, err := rows.SliceScan()
			if err != nil {
				return nil, fmt.Errorf("Failed to scan row: %v", err)
			}
			rowsSlice = append(rowsSlice, r)
			if len(rowsSlice) >= maxResultRows {
				break
			}
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
			return nil, fmt.Errorf("Failed to marshal result: %v", err)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				mcp.TextContent{
					Type: "text",
					Text: b.String(),
				},
			},
		}, nil
	})
	return server.ServeStdio(mcpServer)
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
