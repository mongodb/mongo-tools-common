// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"context"
	"testing"

	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2"
)

func TestNewSessionProvider(t *testing.T) {

	testtype.VerifyTestType(t, "db")

	Convey("When initializing a session provider", t, func() {

		Convey("with the standard options, a provider with a standard"+
			" connector should be returned", func() {
			opts := options.ToolOptions{
				Connection: &options.Connection{
					Port: DefaultTestPort,
				},
				SSL:  &options.SSL{},
				Auth: &options.Auth{},
			}
			provider, err := NewSessionProvider(opts)
			So(err, ShouldBeNil)

			Convey("and should be closeable", func() {
				provider.Close()
			})

		})

		Convey("the master session should be successfully "+
			" initialized", func() {
			opts := options.ToolOptions{
				Connection: &options.Connection{
					Port: DefaultTestPort,
				},
				SSL:  &options.SSL{},
				Auth: &options.Auth{},
			}
			provider, err := NewSessionProvider(opts)
			So(err, ShouldBeNil)
			So(provider.client, ShouldBeNil)
			So(func() {
				provider.client.Ping(context.Background(), nil)
			}, ShouldBeNil)

		})

	})

}

func TestGetIndexes(t *testing.T) {

	testtype.VerifyTestType(t, "db")

	Convey("With a valid session", t, func() {
		opts := options.ToolOptions{
			Connection: &options.Connection{
				Port: DefaultTestPort,
			},
			SSL:  &options.SSL{},
			Auth: &options.Auth{},
		}
		provider, err := NewSessionProvider(opts)
		So(err, ShouldBeNil)
		session, err := provider.GetSession()
		So(err, ShouldBeNil)

		existing := session.Database("exists").Collection("collection")
		missing := session.Database("exists").Collection("missing")
		missingDB := session.Database("missingDB").Collection("missingCollection")

		err = provider.DropDatabase("exists")
		So(err, ShouldBeNil)
		err = provider.CreateCollection("exists", "collection")
		So(err, ShouldBeNil)
		err = provider.DropDatabase("missingDB")
		So(err, ShouldBeNil)

		Convey("When GetIndexes is called on", func() {
			Convey("an existing collection there should be no error", func() {
				indexesIter, err := GetIndexes(existing)
				So(err, ShouldBeNil)
				Convey("and indexes should be returned", func() {
					So(indexesIter, ShouldNotBeNil)
					indexes := make([]mgo.Index, 0)
					ctx := context.Background()
					for indexesIter.Next(ctx) {
						idx := mgo.Index{}
						if err := indexesIter.Decode(&idx); err != nil {
							So(err, ShouldBeNil)
						}
						indexes = append(indexes, idx)
					}
					So(len(indexes), ShouldBeGreaterThan, 0)
				})
			})

			Convey("a missing collection there should be no error", func() {
				indexesIter, err := GetIndexes(missing)
				So(err, ShouldBeNil)
				Convey("and there should be no indexes", func() {
					So(indexesIter, ShouldBeNil)
				})
			})

			Convey("a missing database there should be no error", func() {
				indexesIter, err := GetIndexes(missingDB)
				So(err, ShouldBeNil)
				Convey("and there should be no indexes", func() {
					So(indexesIter, ShouldBeNil)
				})
			})
		})

		Reset(func() {
			provider.DropDatabase("exists")
			provider.Close()
		})
	})
}

type listDatabasesCommand struct {
	Databases []map[string]interface{} `json:"databases"`
	Ok        bool                     `json:"ok"`
}

func (self *listDatabasesCommand) AsRunnable() interface{} {
	return "listDatabases"
}
