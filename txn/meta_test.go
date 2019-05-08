// Copyright (C) MongoDB, Inc. 2019-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package txn

import (
	"io/ioutil"
	"testing"

	"github.com/mongodb/mongo-tools-common/db"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	OplogEntriesFile = "testdata/oplog_entries.json"
)

func readTestData(t *testing.T) bson.Raw {
	b, err := ioutil.ReadFile(OplogEntriesFile)
	if err != nil {
		t.Fatalf("Couldn't load %s: %v", OplogEntriesFile, err)
	}
	var data bson.Raw
	err = bson.UnmarshalExtJSON(b, false, &data)
	if err != nil {
		t.Fatalf("Couldn't decode JSON: %v", err)
	}
	return data
}

func getOpsForCase(t *testing.T, name string, data bson.Raw) []db.Oplog {
	rawArray, err := data.LookupErr(name)
	if err != nil {
		t.Fatalf("Couldn't find ops for case %s: %v", name, err)
	}
	rawOps, err := rawArray.Array().Elements()
	if err != nil {
		t.Fatalf("Couldn't extract array elements for case %s: %v", name, err)
	}
	ops := make([]db.Oplog, len(rawOps))
	for i, e := range rawOps {
		err := e.Value().Unmarshal(&ops[i])
		if err != nil {
			t.Fatalf("Couldn't unmarshal op %d for case %s: %v", i, name, err)
		}
	}
	return ops
}

type TestMetaCase struct {
	name       string
	entryCount int
	notTxn     bool
	commits    bool
	aborts     bool
}

func TestTxnMeta(t *testing.T) {
	cases := []TestMetaCase{
		{name: "not transaction", entryCount: 1, notTxn: true},
		{name: "applyops not transaction", entryCount: 1, notTxn: true},
		{name: "small, unprepared", entryCount: 1, commits: true},
		{name: "small, unprepared, 4.0", entryCount: 1, commits: true},
		{name: "large, unprepared", entryCount: 3, commits: true},
		{name: "small, prepared, committed", entryCount: 2, commits: true},
		{name: "small, prepared, aborted", entryCount: 2, aborts: true},
		{name: "large, prepared, committed", entryCount: 4, commits: true},
		{name: "large, prepared, aborted", entryCount: 4, aborts: true},
	}

	data := readTestData(t)
	for _, c := range cases {
		t.Run(c.name, func(*testing.T) {
			if c.notTxn {
				runNonTxnMetaCase(t, c, data)
			} else {
				runTxnMetaCase(t, c, data)
			}
		})
	}
}

func runNonTxnMetaCase(t *testing.T, c TestMetaCase, data bson.Raw) {
	ops := getOpsForCase(t, c.name, data)

	meta, err := NewMeta(ops[0])
	if err != nil {
		t.Fatalf("case %s: failed to parse op: %v", c.name, err)
	}

	if meta.IsTxn() {
		t.Errorf("case %s: non-txn meta looks like transaction", c.name)
	}

	return
}

func runTxnMetaCase(t *testing.T, c TestMetaCase, data bson.Raw) {
	ops := getOpsForCase(t, c.name, data)

	// Double check that we get all the ops we expected.
	if len(ops) != c.entryCount {
		t.Errorf("case %s: expected %d ops, but got %d", c.name, c.entryCount, len(ops))
	}

	// Test properties of each op.
	for i, o := range ops {
		meta, err := NewMeta(o)
		if err != nil {
			t.Fatalf("case %s [%d]: failed to parse op: %v", c.name, i, err)
		}

		if (meta.id == ID{}) {
			t.Errorf("case %s [%d]: Id was zero value", c.name, i)
		}

		isMultiOp := c.entryCount > 1
		if meta.IsMultiOp() != isMultiOp {
			t.Errorf("case %s [%d]: expected IsMultiOp %v, but got %v", c.name, i, meta.IsMultiOp(), isMultiOp)
		}

		if i != len(ops)-1 {
			if meta.IsFinal() {
				t.Errorf("case %s [%d]: op parsed as final, but it wasn't", c.name, i)
			}
		}
	}

	// Test properties of the last op.
	lastOp := ops[len(ops)-1]
	meta, _ := NewMeta(lastOp)

	if !meta.IsFinal() {
		t.Errorf("case %s: last oplog entry not marked final", c.name)
	}

	if c.commits && !meta.IsCommit() {
		t.Errorf("case %s: expected last oplog entry to be a commit but it wasn't", c.name)
	}
	if c.aborts && !meta.IsAbort() {
		t.Errorf("case %s: expected last oplog entry to be a abort but it wasn't", c.name)
	}
}
