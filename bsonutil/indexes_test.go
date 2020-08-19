// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package bsonutil

import (
	"github.com/mongodb/mongo-tools-common/testtype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
)

func TestIsIndexKeysEqual(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	tests := []struct {
		IndexKeys1 bson.D
		IndexKeys2 bson.D
		Expected   bool
	}{
		{bson.D{{"a", 1}, {"b", int32(1)}},
			bson.D{{"a", float64(1)}, {"b", int64(1)}},
			true},
		{bson.D{{"a", "1"}, {"b", "1"}},
			bson.D{{"a", 1}, {"b", int32(1)}},
			true},
		{bson.D{{"a", -1.0}, {"b", "1.0"}},
			bson.D{{"a", -1}, {"b", int32(1)}},
			true},
		{bson.D{{"a", -2.0}},
			bson.D{{"a", -1}},
			false},
		{bson.D{{"a", "1.1"}},
			bson.D{{"a", 1}},
			false},
		{bson.D{{"b", int32(1)}},
			bson.D{{"a", int32(1)}},
			false},
		{bson.D{{"a", int32(1)}, {"b", int32(1)}},
			bson.D{{"b", int32(1)}, {"a", int32(1)}},
			false},
	}

	for _, test := range tests {
		if result := IsIndexKeysEqual(test.IndexKeys1, test.IndexKeys2); result != test.Expected {
			t.Fatalf("Wrong output from IsIndexKeysEqual as expected, test: %v, actual: %v", test, result)
		}
	}
}

func TestConvertLegacyIndexKeys(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("Converting legacy Indexes", t, func() {
		index1Key := bson.D{{"foo", 0}, {"int32field", int32(2)},
			{"int64field", int64(-3)}, {"float64field", float64(-1)}, {"float64field", float64(-1.1)}}
		ConvertLegacyIndexKeys(index1Key, "test")
		So(index1Key, ShouldResemble, bson.D{{"foo", 1}, {"int32field", int32(2)}, {"int64field", int64(-3)},
			{"float64field", float64(-1)}, {"float64field", float64(-1.1)}})

		decimalNOne, _ := primitive.ParseDecimal128("-1")
		decimalZero, _ := primitive.ParseDecimal128("0")
		decimalOne, _ := primitive.ParseDecimal128("1")
		decimalZero1, _ := primitive.ParseDecimal128("0.00")
		index2Key := bson.D{{"key1", decimalNOne}, {"key2", decimalZero}, {"key3", decimalOne}, {"key4", decimalZero1}}
		ConvertLegacyIndexKeys(index2Key, "test")
		So(index2Key, ShouldResemble, bson.D{{"key1", decimalNOne}, {"key2", int32(1)}, {"key3", decimalOne}, {"key4", decimalZero1}})

		index3Key := bson.D{{"key1", ""}, {"key2", "1"}, {"key3", "-1"}, {"key4", "2dsphere"}}
		ConvertLegacyIndexKeys(index3Key, "test")
		So(index3Key, ShouldResemble, bson.D{{"key1", int32(1)}, {"key2", "1"}, {"key3", "-1"}, {"key4", "2dsphere"}})

		index4Key := bson.D{{"key1", bson.E{"invalid", 1}}, {"key2", primitive.Binary{}}}
		ConvertLegacyIndexKeys(index4Key, "test")
		So(index4Key, ShouldResemble, bson.D{{"key1", int32(1)}, {"key2", int32(1)}})
	})
}
