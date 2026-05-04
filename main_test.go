package main

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestNewSnowflakeTargetTrimsAndRequiresAccountAndRole(t *testing.T) {
	target, err := newSnowflakeTarget("  acct  ", "  role  ", "  warehouse  ")
	if err != nil {
		t.Fatalf("newSnowflakeTarget returned error: %v", err)
	}
	want := snowflakeTarget{
		Account:   "acct",
		Role:      "role",
		Warehouse: "warehouse",
	}
	if target != want {
		t.Fatalf("target = %#v, want %#v", target, want)
	}

	if _, err := newSnowflakeTarget("", "role", "warehouse"); err == nil {
		t.Fatal("missing account returned nil error")
	}
	if _, err := newSnowflakeTarget("acct", "", "warehouse"); err == nil {
		t.Fatal("missing role returned nil error")
	}
}

func TestConnectionManagerCachesByAccountRoleAndWarehouse(t *testing.T) {
	var opened []snowflakeTarget
	manager := newConnectionManager(func(target snowflakeTarget) (*sqlx.DB, error) {
		opened = append(opened, target)
		return &sqlx.DB{}, nil
	})

	first := snowflakeTarget{Account: "acct", Role: "role", Warehouse: "wh1"}
	second := snowflakeTarget{Account: "acct", Role: "role", Warehouse: "wh2"}

	firstDB, err := manager.DB(first)
	if err != nil {
		t.Fatalf("manager.DB(first) returned error: %v", err)
	}
	againDB, err := manager.DB(first)
	if err != nil {
		t.Fatalf("manager.DB(first again) returned error: %v", err)
	}
	secondDB, err := manager.DB(second)
	if err != nil {
		t.Fatalf("manager.DB(second) returned error: %v", err)
	}

	if firstDB != againDB {
		t.Fatal("manager did not reuse db for identical target")
	}
	if firstDB == secondDB {
		t.Fatal("manager reused db for different warehouse")
	}

	wantOpened := []snowflakeTarget{first, second}
	if !reflect.DeepEqual(opened, wantOpened) {
		t.Fatalf("opened = %#v, want %#v", opened, wantOpened)
	}
}

func TestConnectionManagerOpensOnceForConcurrentSameTarget(t *testing.T) {
	target := snowflakeTarget{Account: "acct", Role: "role", Warehouse: "warehouse"}
	openEntered := make(chan struct{})
	releaseOpen := make(chan struct{})
	openCalls := 0

	manager := newConnectionManager(func(target snowflakeTarget) (*sqlx.DB, error) {
		openCalls++
		if openCalls == 1 {
			close(openEntered)
			<-releaseOpen
		}
		return &sqlx.DB{}, nil
	})

	const callers = 8
	var wg sync.WaitGroup
	dbs := make(chan *sqlx.DB, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := manager.DB(target)
			if err != nil {
				errs <- err
				return
			}
			dbs <- db
		}()
	}

	select {
	case <-openEntered:
	case <-time.After(time.Second):
		t.Fatal("connection manager did not start opening a db")
	}
	close(releaseOpen)
	wg.Wait()
	close(dbs)
	close(errs)

	for err := range errs {
		t.Fatalf("manager.DB returned error: %v", err)
	}
	if openCalls != 1 {
		t.Fatalf("open called %d times, want 1", openCalls)
	}
	var first *sqlx.DB
	for db := range dbs {
		if first == nil {
			first = db
			continue
		}
		if db != first {
			t.Fatal("manager returned different dbs for concurrent identical target")
		}
	}
}

func TestConnectionManagerRejectsExcessTargets(t *testing.T) {
	manager := newConnectionManagerWithLimit(func(target snowflakeTarget) (*sqlx.DB, error) {
		return &sqlx.DB{}, nil
	}, 2)

	first := snowflakeTarget{Account: "acct1", Role: "role", Warehouse: "wh"}
	second := snowflakeTarget{Account: "acct2", Role: "role", Warehouse: "wh"}
	third := snowflakeTarget{Account: "acct3", Role: "role", Warehouse: "wh"}

	if _, err := manager.DB(first); err != nil {
		t.Fatalf("manager.DB(first) returned error: %v", err)
	}
	if _, err := manager.DB(second); err != nil {
		t.Fatalf("manager.DB(second) returned error: %v", err)
	}
	if _, err := manager.DB(first); err != nil {
		t.Fatalf("manager.DB(first again) returned error: %v", err)
	}
	if _, err := manager.DB(third); err == nil {
		t.Fatal("manager.DB(third) returned nil error after target limit")
	} else if !strings.Contains(err.Error(), "maximum is 2") {
		t.Fatalf("manager.DB(third) error = %q, want target limit", err.Error())
	}
}

func TestNewQueryToolResultIncludesStructuredMetadata(t *testing.T) {
	resultData := queryResult{
		ColumnInfo: []queryColumn{
			{Name: "ID", Type: "NUMBER"},
		},
		Rows:            [][]any{{float64(1)}},
		ReturnedRows:    1,
		RowLimit:        maxResultRows,
		ResultByteLimit: maxResultBytes,
		CellByteLimit:   maxCellBytes,
		Truncated:       true,
		TruncatedReason: truncatedByRowLimit,
		Notice:          "Only first 1000 rows are shown",
	}
	if err := fitQueryResultToBudget(&resultData); err != nil {
		t.Fatalf("fitQueryResultToBudget returned error: %v", err)
	}

	result, err := newQueryToolResult(resultData)
	if err != nil {
		t.Fatalf("newQueryToolResult returned error: %v", err)
	}
	if result.IsError {
		t.Fatal("query result was marked as an error")
	}
	if !reflect.DeepEqual(result.StructuredContent, resultData) {
		t.Fatalf("StructuredContent = %#v, want %#v", result.StructuredContent, resultData)
	}
	if len(result.Content) != 1 {
		t.Fatalf("len(Content) = %d, want 1", len(result.Content))
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("Content[0] = %T, want mcp.TextContent", result.Content[0])
	}
	for _, want := range []string{"Returned 1 rows", "truncated=true", "reason=row_limit"} {
		if !strings.Contains(text.Text, want) {
			t.Fatalf("text content does not contain %q: %s", want, text.Text)
		}
	}
	if strings.Contains(text.Text, `"rows"`) {
		t.Fatalf("text content should be a compact summary, got: %s", text.Text)
	}
}

func TestFitQueryResultToBudgetTruncatesOversizedResponse(t *testing.T) {
	resultData := queryResult{
		ColumnInfo: []queryColumn{
			{Name: "PAYLOAD", Type: "TEXT"},
		},
		RowLimit: maxResultRows,
	}
	for i := 0; i < maxResultRows; i++ {
		resultData.Rows = append(resultData.Rows, []any{strings.Repeat("x", 7000)})
	}

	if err := fitQueryResultToBudget(&resultData); err != nil {
		t.Fatalf("fitQueryResultToBudget returned error: %v", err)
	}
	if resultData.ResultBytes > maxResultBytes {
		t.Fatalf("ResultBytes = %d, want <= %d", resultData.ResultBytes, maxResultBytes)
	}
	if !resultData.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	if resultData.TruncatedReason != truncatedByResultSize {
		t.Fatalf("TruncatedReason = %q, want %q", resultData.TruncatedReason, truncatedByResultSize)
	}
	if resultData.ReturnedRows != len(resultData.Rows) {
		t.Fatalf("ReturnedRows = %d, len(Rows) = %d", resultData.ReturnedRows, len(resultData.Rows))
	}
}

func TestRowHasOversizedCell(t *testing.T) {
	oversized, err := rowHasOversizedCell([]any{strings.Repeat("x", maxCellBytes+1)})
	if err != nil {
		t.Fatalf("rowHasOversizedCell returned error: %v", err)
	}
	if !oversized {
		t.Fatal("rowHasOversizedCell = false, want true")
	}

	oversized, err = rowHasOversizedCell([]any{strings.Repeat("x", maxCellBytes)})
	if err != nil {
		t.Fatalf("rowHasOversizedCell returned error: %v", err)
	}
	if oversized {
		t.Fatal("rowHasOversizedCell = true at exact limit, want false")
	}
}

func TestParseArgsUnknownSnowflakeFlagGivesHint(t *testing.T) {
	action, err := parseArgs([]string{"--account", "acct"})
	if action != parseActionExit {
		t.Fatalf("action = %v, want %v", action, parseActionExit)
	}
	if err == nil {
		t.Fatal("parseArgs returned nil error")
	}
	for _, want := range []string{"unknown option --account", "MCP query tool arguments"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("parseArgs error does not contain %q: %s", want, err.Error())
		}
	}
}

func TestParseArgsVersionExitsSuccessfully(t *testing.T) {
	action, err := parseArgs([]string{"--version"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}
	if action != parseActionExit {
		t.Fatalf("action = %v, want %v", action, parseActionExit)
	}
	if got := versionString(); !strings.Contains(got, "snowflake-mcp 1.0.0 commit=") {
		t.Fatalf("versionString() = %q", got)
	}
}

func TestQueryTimeoutErrorIsActionable(t *testing.T) {
	err := queryTimeoutError(snowflakeTarget{Account: "acct", Role: "role", Warehouse: "wh"})
	for _, want := range []string{"query exceeded 5m timeout", "LIMIT", "filters"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("queryTimeoutError() does not contain %q: %s", want, err.Error())
		}
	}
}
