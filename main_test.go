package main

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
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
