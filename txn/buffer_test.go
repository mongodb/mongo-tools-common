package txn

import (
	"math/rand"
	"testing"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/testtype"
)

// test each type of transaction individually and serially
func TestSingleTxnBuffer(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	buffer := NewBuffer()
	txnByID, err := mapTestTxnByID()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			testBufferOps(t, buffer, c.ops, txnByID)
		})
	}
}

func TestMixedTxnBuffer(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	buffer := NewBuffer()
	txnByID, err := mapTestTxnByID()
	if err != nil {
		t.Fatal(err)
	}

	// Copy initial ops so we can destructively shuffle them together
	streams := make([][]db.Oplog, len(testCases))
	opCount := 0
	for i, c := range testCases {
		streams[i] = make([]db.Oplog, len(c.ops))
		copy(streams[i], c.ops)
		opCount += len(c.ops)
	}

	ops := make([]db.Oplog, 0, opCount)
	for len(streams) != 0 {
		// randomly pick a stream to add an op
		rand.Shuffle(len(streams), func(i, j int) {
			streams[i], streams[j] = streams[j], streams[i]
		})
		ops = append(ops, streams[0][0])
		// remove the op and its stream if empty
		streams[0] = streams[0][1:]
		if len(streams[0]) == 0 {
			streams = streams[1:]
		}
	}

	testBufferOps(t, buffer, ops, txnByID)
}

func testBufferOps(t *testing.T, buffer *Buffer, ops []db.Oplog, txnByID map[ID]*TestData) {

	innerOpCounter := make(map[ID]int)

	for _, op := range ops {
		meta, _ := NewMeta(op)

		if !meta.IsTxn() {
			return
		}

		err := buffer.AddOp(meta, op)
		if err != nil {
			t.Fatalf("AddOp failed: %v", err)
		}

		if meta.IsAbort() {
			err := buffer.PurgeTxn(meta)
			if err != nil {
				t.Fatalf("PurgeTxn (abort) failed: %v", err)
			}
			assertNoStateForID(t, meta, buffer)
			continue
		}

		if !meta.IsCommit() {
			continue
		}

		// From here, we're simulating "applying" transaction entries
		ops, errs := buffer.GetTxnStream(meta)

	LOOP:
		for {
			select {
			case _, ok := <-ops:
				if !ok {
					break LOOP
				}
				innerOpCounter[meta.id]++
			case err := <-errs:
				if err != nil {
					t.Fatalf("GetTxnStream streaming failed: %v", err)
				}
				break LOOP
			}
		}

		expectedCnt := txnByID[meta.id].innerOpCount
		if innerOpCounter[meta.id] != expectedCnt {
			t.Errorf("incorrect streamed op count; got %d, expected %d", innerOpCounter[meta.id], expectedCnt)
		}

		err = buffer.PurgeTxn(meta)
		if err != nil {
			t.Fatalf("PurgeTxn (commit) failed: %v", err)
		}
		assertNoStateForID(t, meta, buffer)

	}

}

func assertNoStateForID(t *testing.T, meta Meta, buffer *Buffer) {
	_, ok := buffer.txns[meta.id]
	if ok {
		t.Errorf("state not cleared for %v", meta.id)
	}
}
