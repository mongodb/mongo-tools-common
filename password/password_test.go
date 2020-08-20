package password

import (
	"bytes"
	"github.com/mongodb/mongo-tools-common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

const (
	testPwd = "test_pwd"
)

func TestPasswordFromNonTerminal(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)
	Convey("stdin is not a terminal", t, func() {
		var b bytes.Buffer

		b.WriteString(testPwd)
		reader := bytes.NewReader(b.Bytes())

		pass, err := readPassNonInteractively(reader)
		So(err, ShouldBeNil)
		So(pass, ShouldEqual, testPwd)
	})
}
