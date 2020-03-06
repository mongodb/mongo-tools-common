// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package options

import (
	"fmt"
	"os"
	"strings"

	"github.com/mongodb/mongo-tools-common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"

	"runtime"
	"testing"
	"time"
)

const (
	ShouldSucceed = iota
	ShouldFail
)

func TestVerbosityFlag(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("With a new ToolOptions", t, func() {
		enabled := EnabledOptions{false, false, false, false}
		optPtr := New("", "", "", "", enabled)
		So(optPtr, ShouldNotBeNil)
		So(optPtr.parser, ShouldNotBeNil)

		Convey("no verbosity flags, Level should be 0", func() {
			_, err := optPtr.parser.ParseArgs([]string{})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 0)
		})

		Convey("one short verbosity flag, Level should be 1", func() {
			_, err := optPtr.parser.ParseArgs([]string{"-v"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 1)
		})

		Convey("three short verbosity flags (consecutive), Level should be 3", func() {
			_, err := optPtr.parser.ParseArgs([]string{"-vvv"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 3)
		})

		Convey("three short verbosity flags (dispersed), Level should be 3", func() {
			_, err := optPtr.parser.ParseArgs([]string{"-v", "-v", "-v"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 3)
		})

		Convey("short verbosity flag assigned to 3, Level should be 3", func() {
			_, err := optPtr.parser.ParseArgs([]string{"-v=3"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 3)
		})

		Convey("consecutive short flags with assignment, only assignment holds", func() {
			_, err := optPtr.parser.ParseArgs([]string{"-vv=3"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 3)
		})

		Convey("one long verbose flag, Level should be 1", func() {
			_, err := optPtr.parser.ParseArgs([]string{"--verbose"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 1)
		})

		Convey("three long verbosity flags, Level should be 3", func() {
			_, err := optPtr.parser.ParseArgs([]string{"--verbose", "--verbose", "--verbose"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 3)
		})

		Convey("long verbosity flag assigned to 3, Level should be 3", func() {
			_, err := optPtr.parser.ParseArgs([]string{"--verbose=3"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 3)
		})

		Convey("mixed assignment and bare flag, total is sum", func() {
			_, err := optPtr.parser.ParseArgs([]string{"--verbose", "--verbose=3"})
			So(err, ShouldBeNil)
			So(optPtr.Level(), ShouldEqual, 4)
		})
	})
}

type uriTester struct {
	Name                 string
	CS                   connstring.ConnString
	OptsIn               *ToolOptions
	OptsExpected         *ToolOptions
	WithSSL              bool
	WithGSSAPI           bool
	ShouldError          bool
	ShouldAskForPassword bool
}

func TestParseAndSetOptions(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("With a matrix of URIs and expected results", t, func() {
		enabledURIOnly := EnabledOptions{false, false, false, true}
		testCases := []uriTester{
			{
				Name: "not built with ssl",
				CS: connstring.ConnString{
					SSL:    true,
					SSLSet: true,
				},
				WithSSL:      false,
				OptsIn:       New("", "", "", "", enabledURIOnly),
				OptsExpected: New("", "", "", "", enabledURIOnly),
				ShouldError:  true,
			},
			{
				Name: "not built with ssl using SRV",
				CS: connstring.ConnString{
					SSL:      true,
					SSLSet:   true,
					Original: "mongodb+srv://example.com/",
				},
				WithSSL:      false,
				OptsIn:       New("", "", "", "", enabledURIOnly),
				OptsExpected: New("", "", "", "", enabledURIOnly),
				ShouldError:  true,
			},
			{
				Name: "built with ssl",
				CS: connstring.ConnString{
					SSL:    true,
					SSLSet: true,
				},
				WithSSL: true,
				OptsIn:  New("", "", "", "", enabledURIOnly),
				OptsExpected: &ToolOptions{
					General:    &General{},
					Verbosity:  &Verbosity{},
					Connection: &Connection{},
					URI:        &URI{},
					SSL: &SSL{
						UseSSL: true,
					},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: enabledURIOnly,
				},
				ShouldError: false,
			},
			{
				Name: "built with ssl using SRV",
				CS: connstring.ConnString{
					SSL:      true,
					SSLSet:   true,
					Original: "mongodb+srv://example.com/",
				},
				WithSSL: true,
				OptsIn:  New("", "", "", "", enabledURIOnly),
				OptsExpected: &ToolOptions{
					General:    &General{},
					Verbosity:  &Verbosity{},
					Connection: &Connection{},
					URI:        &URI{},
					SSL: &SSL{
						UseSSL: true,
					},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: enabledURIOnly,
				},
				ShouldError: false,
			},
			{
				Name: "not built with gssapi",
				CS: connstring.ConnString{
					AuthMechanism: "GSSAPI",
				},
				WithGSSAPI:   false,
				OptsIn:       New("", "", "", "", enabledURIOnly),
				OptsExpected: New("", "", "", "", enabledURIOnly),
				ShouldError:  true,
			},
			{
				Name: "built with gssapi",
				CS: connstring.ConnString{
					AuthMechanism: "GSSAPI",
					AuthMechanismProperties: map[string]string{
						"SERVICE_NAME": "service",
					},
					AuthMechanismPropertiesSet: true,
				},
				WithGSSAPI: true,
				OptsIn:     New("", "", "", "", enabledURIOnly),
				OptsExpected: &ToolOptions{
					General:    &General{},
					Verbosity:  &Verbosity{},
					Connection: &Connection{},
					URI:        &URI{},
					SSL:        &SSL{},
					Auth:       &Auth{},
					Namespace:  &Namespace{},
					Kerberos: &Kerberos{
						Service: "service",
					},
					enabledOptions: enabledURIOnly,
				},
				ShouldError: false,
			},
			{
				Name: "connection fields set",
				CS: connstring.ConnString{
					ConnectTimeout:    time.Duration(100) * time.Millisecond,
					ConnectTimeoutSet: true,
					SocketTimeout:     time.Duration(200) * time.Millisecond,
					SocketTimeoutSet:  true,
				},
				OptsIn: &ToolOptions{
					General:   &General{},
					Verbosity: &Verbosity{},
					Connection: &Connection{
						Timeout: 3, // The default value
					},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Connection: true, URI: true},
				},
				OptsExpected: &ToolOptions{
					General:   &General{},
					Verbosity: &Verbosity{},
					Connection: &Connection{
						Timeout:       100,
						SocketTimeout: 200,
					},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Connection: true, URI: true},
				},
				ShouldError: false,
			},
			{
				Name: "auth fields set",
				CS: connstring.ConnString{
					AuthMechanism: "MONGODB-X509",
					AuthSource:    "",
					AuthSourceSet: true,
					Username:      "user",
					Password:      "password",
				},
				OptsIn: &ToolOptions{
					General:        &General{},
					Verbosity:      &Verbosity{},
					Connection:     &Connection{},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Auth: true, URI: true},
				},
				OptsExpected: &ToolOptions{
					General:    &General{},
					Verbosity:  &Verbosity{},
					Connection: &Connection{},
					URI:        &URI{},
					SSL:        &SSL{},
					Auth: &Auth{
						Username:  "user",
						Password:  "password",
						Source:    "",
						Mechanism: "MONGODB-X509",
					},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Connection: true, URI: true},
				},
				ShouldError: false,
			},
			{
				Name: "should ask for password",
				CS: connstring.ConnString{
					AuthMechanism: "MONGODB-X509",
					AuthSource:    "",
					Username:      "user",
				},
				OptsIn: &ToolOptions{
					General:        &General{},
					Verbosity:      &Verbosity{},
					Connection:     &Connection{},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Auth: true, URI: true},
				},
				OptsExpected: &ToolOptions{
					General:    &General{},
					Verbosity:  &Verbosity{},
					Connection: &Connection{},
					URI:        &URI{},
					SSL:        &SSL{},
					Auth: &Auth{
						Username:  "user",
						Source:    "",
						Mechanism: "MONGODB-X509",
					},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Connection: true, URI: true},
				},
				ShouldError:          false,
				ShouldAskForPassword: true,
			},
			{
				Name: "single connect sets 'Direct'",
				CS: connstring.ConnString{
					Connect: connstring.SingleConnect,
				},
				OptsIn: New("", "", "", "", enabledURIOnly),
				OptsExpected: &ToolOptions{
					General:        &General{},
					Verbosity:      &Verbosity{},
					Connection:     &Connection{},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Direct:         true,
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{URI: true},
				},
				ShouldError: false,
			},
			{
				Name: "ReplSetName is set when CS contains it",
				CS: connstring.ConnString{
					ReplicaSet: "replset",
				},
				OptsIn: New("", "", "", "", enabledURIOnly),
				OptsExpected: &ToolOptions{
					General:        &General{},
					Verbosity:      &Verbosity{},
					Connection:     &Connection{},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{URI: true},
					ReplicaSetName: "replset",
				},
				ShouldError: false,
			},
			{
				Name: "Don't fail when uri and options set",
				CS: connstring.ConnString{
					Hosts: []string{"host"},
				},
				OptsIn: &ToolOptions{
					General:   &General{},
					Verbosity: &Verbosity{},
					Connection: &Connection{
						Host: "host",
					},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Connection: true, URI: true},
				},
				OptsExpected: &ToolOptions{
					General:   &General{},
					Verbosity: &Verbosity{},
					Connection: &Connection{
						Host: "host",
					},
					URI:            &URI{},
					SSL:            &SSL{},
					Auth:           &Auth{},
					Namespace:      &Namespace{},
					Kerberos:       &Kerberos{},
					enabledOptions: EnabledOptions{Connection: true, URI: true},
				},
				ShouldError: false,
			},
		}

		Convey("results should match expected", func() {
			for _, testCase := range testCases {
				t.Log("Test Case:", testCase.Name)

				testCase.OptsIn.URI.ConnectionString = "mongodb://dummy"
				testCase.OptsExpected.URI.ConnectionString = "mongodb://dummy"

				BuiltWithSSL = testCase.WithSSL
				BuiltWithGSSAPI = testCase.WithGSSAPI
				defer func() {
					BuiltWithSSL = true
					BuiltWithGSSAPI = true
				}()

				testCase.OptsIn.URI.connString = testCase.CS

				err := testCase.OptsIn.setOptionsFromURI(testCase.CS)

				if testCase.ShouldError {
					So(err, ShouldNotBeNil)
				} else {
					So(err, ShouldBeNil)
				}

				So(testCase.OptsIn.Connection.Timeout, ShouldResemble, testCase.OptsExpected.Connection.Timeout)
				So(testCase.OptsIn.Connection.SocketTimeout, ShouldResemble, testCase.OptsExpected.Connection.SocketTimeout)
				So(testCase.OptsIn.Username, ShouldResemble, testCase.OptsExpected.Username)
				So(testCase.OptsIn.Password, ShouldResemble, testCase.OptsExpected.Password)
				So(testCase.OptsIn.Source, ShouldResemble, testCase.OptsExpected.Source)
				So(testCase.OptsIn.Auth.Mechanism, ShouldResemble, testCase.OptsExpected.Auth.Mechanism)
				So(testCase.OptsIn.Direct, ShouldResemble, testCase.OptsExpected.Direct)
				So(testCase.OptsIn.ReplicaSetName, ShouldResemble, testCase.OptsExpected.ReplicaSetName)
				So(testCase.OptsIn.SSL.UseSSL, ShouldResemble, testCase.OptsExpected.SSL.UseSSL)
				So(testCase.OptsIn.Kerberos.Service, ShouldResemble, testCase.OptsExpected.Kerberos.Service)
				So(testCase.OptsIn.Kerberos.ServiceHost, ShouldResemble, testCase.OptsExpected.Kerberos.ServiceHost)
				So(testCase.OptsIn.Auth.ShouldAskForPassword(), ShouldEqual, testCase.OptsIn.ShouldAskForPassword())
			}
		})
	})
}

type optionsTester struct {
	options string
	uri     string
	outcome int
}

func createOptionsTestCases(s []string) []optionsTester {
	return []optionsTester{
		{fmt.Sprintf("%s %s", s[0], s[2]), "mongodb://user:pass@foo", ShouldSucceed},
		{fmt.Sprintf("%s %s", s[0], s[2]), fmt.Sprintf("mongodb://user:pass@foo/?%s=%s", s[1], s[2]), ShouldSucceed},
		{fmt.Sprintf("%s %s", s[0], s[2]), fmt.Sprintf("mongodb://user:pass@foo/?%s=%s", s[1], s[3]), ShouldFail},
		{"", fmt.Sprintf("mongodb://user:pass@foo/?%s=%s", s[1], s[2]), ShouldSucceed},
	}
}

func runOptionsTestCases(t *testing.T, testCases []optionsTester) {
	enabled := EnabledOptions{
		Auth:       true,
		Connection: true,
		Namespace:  true,
		URI:        true,
	}

	for _, c := range testCases {
		toolOptions := New("test", "", "", "", enabled)
		argString := fmt.Sprintf("%s --uri %s", c.options, c.uri)
		t.Logf("Test Case: %s\n", argString)
		args := strings.Split(argString, " ")
		_, err := toolOptions.ParseArgs(args)
		if c.outcome == ShouldFail {
			So(err, ShouldNotBeNil)
		} else {
			So(err, ShouldBeNil)
		}
	}
}

func TestOptionsParsing(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("With a list of CLI options and URIs", t, func() {
		// handwritten test cases
		specialTestCases := []optionsTester{
			// Hosts and Ports
			{"--host foo", "mongodb://foo", ShouldSucceed},
			{"--host foo", "mongodb://bar", ShouldFail},
			{"--port 27018", "mongodb://foo", ShouldSucceed},
			{"--port 27018", "mongodb://foo:27017", ShouldFail},
			{"--port 27018", "mongodb://foo:27019", ShouldFail},
			{"--port 27018", "mongodb://foo:27018", ShouldSucceed},
			{"--host foo:27019 --port 27018", "mongodb://foo", ShouldFail},
			{"--host foo:27018 --port 27018", "mongodb://foo:27018", ShouldSucceed},
			{"--host foo:27019 --port 27018", "mongodb://foo:27018", ShouldFail},

			{"--host foo,bar,baz", "mongodb://foo,bar,baz", ShouldSucceed},
			{"--host foo,bar,baz", "mongodb://baz,bar,foo", ShouldSucceed},
			{"--host foo:27018,bar:27019,baz:27020", "mongodb://baz:27020,bar:27019,foo:27018", ShouldSucceed},
			{"--host foo:27018,bar:27019,baz:27020", "mongodb://baz:27018,bar:27019,foo:27020", ShouldFail},
			{"--host foo:27018,bar:27019,baz:27020 --port 27018", "mongodb://baz:27018,bar:27019,foo:27020", ShouldFail},
			{"--host foo,bar,baz --port 27018", "mongodb://foo,bar,baz", ShouldSucceed},
			{"--host foo,bar,baz --port 27018", "mongodb://foo:27018,bar:27018,baz:27018", ShouldSucceed},
			{"--host foo,bar,baz --port 27018", "mongodb://foo:27018,bar:27019,baz:27020", ShouldFail},

			{"--host repl/foo,bar,baz", "mongodb://foo,bar,baz", ShouldSucceed},
			{"--host repl/foo,bar,baz", "mongodb://foo,bar,baz/?replicaSet=repl", ShouldSucceed},
			{"--host repl/foo,bar,baz", "mongodb://foo,bar,baz/?replicaSet=quux", ShouldFail},

			// Compressors
			{"--compressors snappy", "mongodb://foo/?compressors=snappy", ShouldSucceed},
			{"", "mongodb://foo/?compressors=snappy", ShouldSucceed},
			{"--compressors snappy", "mongodb://foo/", ShouldSucceed},
			{"--compressors snappy", "mongodb://foo/?compressors=zlib", ShouldFail},
			// {"--compressors none", "mongodb://foo/?compressors=snappy", ShouldFail}, // Note: zero value problem
			{"--compressors snappy", "mongodb://foo/?compressors=none", ShouldFail},

			// Auth
			{"--username alice", "mongodb://alice@foo", ShouldSucceed},
			{"--username alice", "mongodb://foo", ShouldSucceed},
			{"--username bob", "mongodb://alice@foo", ShouldFail},
			{"", "mongodb://alice@foo", ShouldSucceed},

			{"--password hunter2", "mongodb://alice@foo", ShouldSucceed},
			{"--password hunter2", "mongodb://alice:hunter2@foo", ShouldSucceed},
			{"--password hunter2", "mongodb://alice:swordfish@foo", ShouldFail},
			{"", "mongodb://alice:hunter2@foo", ShouldSucceed},

			{"--authenticationDatabase db1", "mongodb://user:pass@foo", ShouldSucceed},
			{"--authenticationDatabase db1", "mongodb://user:pass@foo/?authSource=db1", ShouldSucceed},
			{"--authenticationDatabase db1", "mongodb://user:pass@foo/?authSource=db2", ShouldFail},
			{"", "mongodb://user:pass@foo/?authSource=db1", ShouldSucceed},
			{"--authenticationDatabase db1", "mongodb://user:pass@foo/db2", ShouldSucceed},
			{"--authenticationDatabase db1", "mongodb://user:pass@foo/db2?authSource=db1", ShouldSucceed},
			{"--authenticationDatabase db1", "mongodb://user:pass@foo/db1?authSource=db2", ShouldFail},

			// Namespace
			{"--db db1", "mongodb://foo", ShouldSucceed},
			{"--db db1", "mongodb://foo/db1", ShouldSucceed},
			{"--db db1", "mongodb://foo/db2", ShouldFail},
			{"", "mongodb://foo/db1", ShouldSucceed},
			{"--db db1", "mongodb://user:pass@foo/?authSource=db2", ShouldSucceed},
			{"--db db1", "mongodb://user:pass@foo/db1?authSource=db2", ShouldSucceed},
			{"--db db1", "mongodb://user:pass@foo/db2?authSource=db2", ShouldFail},

			// Kerberos
			{"--gssapiServiceName foo", "mongodb://user:pass@foo/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:foo", ShouldSucceed},
			{"", "mongodb://user:pass@foo/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:foo", ShouldSucceed},
			{"--gssapiServiceName foo", "mongodb://user:pass@foo/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:bar", ShouldFail},
			{"--gssapiServiceName foo", "mongodb://user:pass@foo/?authMechanism=GSSAPI", ShouldSucceed},
		}

		// Each entry is expanded into 4 test cases with createTestCases()
		genericTestCases := [][]string{
			{"--serverSelectionTimeout", "serverSelectionTimeoutMS", "1000", "2000"},
			{"--dialTimeout", "connectTimeoutMS", "1000", "2000"},
			{"--socketTimeout", "socketTimeoutMS", "1000", "2000"},

			{"--authenticationMechanism", "authMechanism", "SCRAM-SHA-1", "GSSAPI"},

			{"--ssl", "ssl", "true", "false"},
			{"--ssl", "tls", "true", "false"},

			{"--sslCAFile", "sslCertificateAuthorityFile", "foo", "bar"},
			{"--sslCAFile", "tlsCAFile", "foo", "bar"},

			{"--sslPEMKeyFile", "sslClientCertificateKeyFile", "foo", "bar"},
			{"--sslPEMKeyFile", "tlsCertificateKeyFile", "foo", "bar"},

			{"--sslPEMKeyPassword", "sslClientCertificateKeyPassword", "foo", "bar"},
			{"--sslPEMKeyPassword", "tlsCertificateKeyFilePassword", "foo", "bar"},

			{"--sslAllowInvalidCertificates", "sslInsecure", "true", "false"},
			{"--sslAllowInvalidCertificates", "tlsInsecure", "true", "false"},

			{"--sslAllowInvalidHostnames", "sslInsecure", "true", "false"},
			{"--sslAllowInvalidHostnames", "tlsInsecure", "true", "false"},
		}

		testCases := []optionsTester{}

		for _, c := range genericTestCases {
			testCases = append(testCases, createOptionsTestCases(c)...)
		}

		testCases = append(testCases, specialTestCases...)

		Convey("parsing should succeed or fail as expected", func() {
			runOptionsTestCases(t, testCases)
		})
	})
}

func TestOptionsParsingForSRV(t *testing.T) {

	testtype.SkipUnlessTestType(t, testtype.SRVConnectionStringTestType)
	atlasURI, ok := os.LookupEnv("ATLAS_URI")
	if !ok {
		t.Errorf("test requires ATLAS_URI to be set")
	}
	cs, err := connstring.Parse(atlasURI)
	if err != nil {
		t.Errorf("Error parsing ATLAS_URI: %s", err)
	}

	Convey("With a list of CLI options and URIs parsing should succeed or fail as expected", t, func() {
		testCases := []optionsTester{
			{"", atlasURI, ShouldSucceed},
			{"--authenticationDatabase admin", atlasURI, ShouldSucceed},
			{"--authenticationDatabase db1", atlasURI, ShouldFail},
			{"--ssl", atlasURI, ShouldSucceed},
			{"--db db1", atlasURI, ShouldSucceed},
			{fmt.Sprintf("--host %s/%s", cs.ReplicaSet, strings.Join(cs.Hosts, ",")), atlasURI, ShouldSucceed},
			{fmt.Sprintf("--host %s/%s", "wrongReplSet", strings.Join(cs.Hosts, ",")), atlasURI, ShouldFail},
		}

		runOptionsTestCases(t, testCases)
	})
}

// Regression test for TOOLS-1694 to prevent issue from TOOLS-1115
func TestHiddenOptionsDefaults(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	Convey("With a ToolOptions parsed", t, func() {
		enabled := EnabledOptions{Connection: true}
		opts := New("", "", "", "", enabled)
		_, err := opts.parser.ParseArgs([]string{})
		So(err, ShouldBeNil)
		Convey("hidden options should have expected values", func() {
			So(opts.MaxProcs, ShouldEqual, runtime.NumCPU())
			So(opts.Timeout, ShouldEqual, 3)
			So(opts.SocketTimeout, ShouldEqual, 0)
		})
	})

}

func TestNamespace_String(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.UnitTestType)

	cases := []struct {
		ns     Namespace
		expect string
	}{
		{Namespace{"foo", "bar"}, "foo.bar"},
		{Namespace{"foo", "bar.baz"}, "foo.bar.baz"},
	}

	for _, c := range cases {
		got := c.ns.String()
		if got != c.expect {
			t.Errorf("invalid string conversion for %#v, got '%s', expected '%s'", c.ns, got, c.expect)
		}
	}

}
