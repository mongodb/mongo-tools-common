package db

import (
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/countopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
)

// DeferredQuery represents a deferred query
type DeferredQuery struct {
	Coll      *mongo.Collection
	Filter    interface{}
	Hint      interface{}
	LogReplay bool
}

func (q *DeferredQuery) Count() (int, error) {
	var opt countopt.Count
	if q.Hint != nil {
		opt = countopt.Hint(q.Hint)
	}
	c, err := q.Coll.CountDocuments(nil, q.Filter, opt)
	return int(c), err
}

func (q *DeferredQuery) Iter() (mongo.Cursor, error) {
	opts := make([]findopt.Find, 0)
	if q.Hint != nil {
		opts = append(opts, findopt.Hint(q.Hint))
	}
	if q.LogReplay {
		opts = append(opts, findopt.OplogReplay(true))
	}
	return q.Coll.Find(nil, q.Filter, opts...)
}

// XXX temporary fix; fake a Repair via regular cursor
func (q *DeferredQuery) Repair() (mongo.Cursor, error) {
	return q.Iter()
}
