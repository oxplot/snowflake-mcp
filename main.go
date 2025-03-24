package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"regexp"

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

	mcpServer := server.NewMCPServer(
		"Snowflake",
		"1.0.0",
		server.WithResourceCapabilities(false, false),
	)

	mcpServer.AddResource(mcp.NewResource(
		"snowflake://",
		"Database list",
		mcp.WithResourceDescription("List of databases"),
		mcp.WithMIMEType("text/plain"),
	), func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		return getNameList(db, "SHOW TERSE DATABASES", func(name string) mcp.ResourceContents {
			return mcp.TextResourceContents{
				URI:      fmt.Sprintf("snowflake://%s", name),
				MIMEType: "text/plain",
				Text:     name,
			}
		})
	})

	schemaPat := regexp.MustCompile(`^snowflake://([^/]+)$`)
	mcpServer.AddResourceTemplate(mcp.NewResourceTemplate(
		"snowflake://{database-name}",
		"Schema list in database",
		mcp.WithTemplateDescription("List of schemas in a database"),
		mcp.WithTemplateMIMEType("text/plain"),
	), func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		m := schemaPat.FindStringSubmatch(request.Params.URI)
		if m == nil {
			return nil, fmt.Errorf("Invalid URI")
		}
		dbName := m[1]
		return getNameList(db, fmt.Sprintf(`SHOW TERSE SCHEMAS IN DATABASE %s`, dbName), func(name string) mcp.ResourceContents {
			return mcp.TextResourceContents{
				URI:      fmt.Sprintf("snowflake://%s/%s", dbName, name),
				MIMEType: "text/plain",
				Text:     name,
			}
		})
	})

	tablesPat := regexp.MustCompile(`^snowflake://([^/]+)/([^/]+)/tables$`)
	mcpServer.AddResourceTemplate(mcp.NewResourceTemplate(
		"snowflake://{database-name}/{schema-name}/tables",
		"Table list in schema",
		mcp.WithTemplateDescription("List of tables in a schema"),
		mcp.WithTemplateMIMEType("text/plain"),
	), func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		m := tablesPat.FindStringSubmatch(request.Params.URI)
		if m == nil {
			return nil, fmt.Errorf("Invalid URI")
		}
		dbName := m[1]
		schemaName := m[2]

		return getNameList(db, fmt.Sprintf(`SHOW TERSE TABLES IN SCHEMA %s.%s`, dbName, schemaName), func(name string) mcp.ResourceContents {
			return mcp.TextResourceContents{
				URI:      fmt.Sprintf("snowflake://%s/%s/table/%s", dbName, schemaName, name),
				MIMEType: "text/plain",
				Text:     name,
			}
		})
	})

	viewsPat := regexp.MustCompile(`^snowflake://([^/]+)/([^/]+)/views$`)
	mcpServer.AddResourceTemplate(mcp.NewResourceTemplate(
		"snowflake://{database-name}/{schema-name}/views",
		"View list in schema",
		mcp.WithTemplateDescription("List of views in a schema"),
		mcp.WithTemplateMIMEType("text/plain"),
	), func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		m := viewsPat.FindStringSubmatch(request.Params.URI)
		if m == nil {
			return nil, fmt.Errorf("Invalid URI")
		}
		dbName, schemaName := m[1], m[2]
		return getNameList(db, fmt.Sprintf(`SHOW TERSE TABLES IN SCHEMA %s.%s`, dbName, schemaName), func(name string) mcp.ResourceContents {
			return mcp.TextResourceContents{
				URI:      fmt.Sprintf("snowflake://%s/%s/view/%s", dbName, schemaName, name),
				MIMEType: "text/plain",
				Text:     name,
			}
		})
	})

	defPat := regexp.MustCompile(`^snowflake://([^/]+)/([^/]+)/(?:view|table)/([^/]+)$`)
	vtDefHandler := func(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		m := defPat.FindStringSubmatch(request.Params.URI)
		if m == nil {
			return nil, fmt.Errorf("Invalid URI")
		}
		dbName, schemaName, tableName := m[1], m[2], m[3]
		rows, err := db.Queryx(fmt.Sprintf("DESCRIBE TABLE %s.%s.%s", dbName, schemaName, tableName))
		if err != nil {
			return nil, fmt.Errorf("Failed to get table def for %s.%s.%s: %v", dbName, schemaName, tableName, err)
		}
		defer rows.Close()

		type column struct {
			Name string `db:"name" json:"name"`
			Type string `db:"type" json:"type"`
			Kind string `db:"kind" json:"-"`
		}

		columns := []column{}
		for rows.Next() {
			t := column{}
			if err = rows.StructScan(&t); err != nil {
				return nil, fmt.Errorf("Failed to scan rows: %v", err)
			}
			if t.Kind != "COLUMN" {
				continue
			}
			columns = append(columns, column(t))
		}

		b := bytes.NewBuffer(nil)
		enc := json.NewEncoder(b)
		enc.SetIndent("", " ")
		if err := enc.Encode(map[string]any{
			"columns": columns,
		}); err != nil {
			return nil, fmt.Errorf("Failed to marshal result: %v", err)
		}

		return []mcp.ResourceContents{
			mcp.TextResourceContents{
				URI:      request.Params.URI,
				MIMEType: "application/json",
				Text:     b.String(),
			},
		}, nil
	}

	mcpServer.AddResourceTemplate(mcp.NewResourceTemplate(
		"snowflake://{database-name}/{schema-name}/table/{table-name}",
		"Table definition",
		mcp.WithTemplateDescription("Definition of a table including columns and column types"),
		mcp.WithTemplateMIMEType("application/json"),
	), vtDefHandler)

	mcpServer.AddResourceTemplate(mcp.NewResourceTemplate(
		"snowflake://{database-name}/{schema-name}/view/{table-name}",
		"View definition",
		mcp.WithTemplateDescription("Definition of a view including columns and column types"),
		mcp.WithTemplateMIMEType("application/json"),
	), vtDefHandler)

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

func getNameList[T any](db *sqlx.DB, query string, conv func(name string) T) ([]T, error) {
	rows, err := db.Queryx(query)
	if err != nil {
		return nil, fmt.Errorf("Failed to run query '%s': %v", query, err)
	}
	defer rows.Close()

	ret := []T{}
	for rows.Next() {
		t := struct {
			Name string `db:"name"`
		}{}
		if err = rows.StructScan(&t); err != nil {
			return nil, fmt.Errorf("Failed to scan rows: %v", err)
		}
		ret = append(ret, conv(t.Name))
	}
	return ret, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
