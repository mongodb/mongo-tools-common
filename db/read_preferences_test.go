// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/tag"
	"go.mongodb.org/mongo-driver/x/network/connstring"
)

func TestNewReadPreference(t *testing.T) {
	tagSet := map[string]string{
		"foo": "bar",
	}
	cs := &connstring.ConnString{
		ReadPreference:        "secondary",
		ReadPreferenceTagSets: []map[string]string{tagSet},
		MaxStaleness:          time.Duration(5) * time.Second,
		MaxStalenessSet:       true,
	}

	Convey("When calling NewReadPreference", t, func() {
		Convey("Not specifying a read preference on either should default to primary", func() {
			pref, err := NewReadPreference("", nil)
			So(err, ShouldBeNil)
			So(pref.Mode(), ShouldEqual, readpref.PrimaryMode)
		})

		Convey("Specifying a read preference only on the command line should set it correctly", func() {
			rp := "{\"mode\": \"secondary\", \"tags\": {\"foo\": \"bar\"}}"
			pref, err := NewReadPreference(rp, nil)
			So(err, ShouldBeNil)
			So(pref.Mode(), ShouldEqual, readpref.SecondaryMode)
			tagSets := pref.TagSets()
			So(len(tagSets), ShouldEqual, 1)
			So(tagSets[0], ShouldResemble, tag.Set{tag.Tag{Name: "foo", Value: "bar"}})
		})

		Convey("Specifying a read preference only in the URI should set it correctly", func() {
			pref, err := NewReadPreference("", cs)
			So(err, ShouldBeNil)
			So(pref.Mode(), ShouldEqual, readpref.SecondaryMode)
			tagSets := pref.TagSets()
			So(len(tagSets), ShouldEqual, 1)
			So(tagSets[0], ShouldResemble, tag.Set{tag.Tag{Name: "foo", Value: "bar"}})

			maxStaleness, set := pref.MaxStaleness()
			So(set, ShouldBeTrue)
			So(maxStaleness, ShouldEqual, time.Duration(5) * time.Second)
		})

		Convey("Specifying a read preference in the command line and URI should set it to the command line", func() {
			rp := "{\"mode\": \"nearest\", \"tags\": {\"one\": \"two\"}}"
			pref, err := NewReadPreference(rp, cs)
			So(err, ShouldBeNil)
			So(pref.Mode(), ShouldEqual, readpref.NearestMode)
			tagSets := pref.TagSets()
			So(len(tagSets), ShouldEqual, 1)
			So(tagSets[0], ShouldResemble, tag.Set{tag.Tag{Name: "one", Value: "two"}})
		})
	})
}
