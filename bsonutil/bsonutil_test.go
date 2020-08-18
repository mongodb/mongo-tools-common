package bsonutil

import (
	"github.com/mongodb/mongo-tools-common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"testing"
)

func TestBson2Float64(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	decimalVal, _ := primitive.ParseDecimal128("-1")
	tests := []struct {
		In        interface{}
		Expected  float64
		isSuccess bool
	}{
		{int8(1), 1.0, true},
		{int16(2), 2.0, true},
		{int32(1), 1.0, true},
		{int64(1), 1.0, true},
		{uint8(1), 1.0, true},
		{uint16(1), 1.0, true},
		{uint32(1), 1.0, true},
		{uint64(1), 1.0, true},
		{1.0, 1.0, true},
		{float32(1.0), 1.0, true},
		{"1", 1.0, true},
		{"1.5", 1.5, true},
		{"invalid", float64(0), false},
		{decimalVal, float64(-1), true},
	}

	Convey("Test numerical value conversion", t, func() {
		for _, test := range tests {
			result, ok := Bson2Float64(test.In)
			So(ok, ShouldEqual, test.isSuccess)
			So(result, ShouldEqual, test.Expected)
		}
	})
}
