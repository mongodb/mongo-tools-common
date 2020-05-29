// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package db

import (
	"context"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"os"
	"testing"

	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/testtype"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
)

// var block and functions copied from testutil to avoid import cycle
var (
	UserAdmin              = "uAdmin"
	UserAdminPassword      = "password"
	CreatedUserNameEnv     = "TOOLS_TESTING_AUTH_USERNAME"
	CreatedUserPasswordEnv = "TOOLS_TESTING_AUTH_PASSWORD"
)

func DBGetAuthOptions() options.Auth {
	if testtype.HasTestType(testtype.AuthTestType) {
		return options.Auth{
			Username: os.Getenv(CreatedUserNameEnv),
			Password: os.Getenv(CreatedUserPasswordEnv),
			Source:   "admin",
		}
	}

	return options.Auth{}
}

func DBGetSSLOptions() options.SSL {
	if testtype.HasTestType(testtype.SSLTestType) {
		return options.SSL{
			UseSSL:        true,
			SSLCAFile:     "../db/testdata/ca-ia.pem",
			SSLPEMKeyFile: "../db/testdata/test-client.pem",
		}
	}

	return options.SSL{
		UseSSL: false,
	}
}

func DBGetConnString() *options.URI {
	if testtype.HasTestType(testtype.SSLTestType) {
		return &options.URI{
			//ConnectionString: "mongodb://localhost" + DefaultTestPort + "/",
			ConnString: connstring.ConnString{
				SSLCaFileSet:                   true,
				SSLCaFile:                      "../db/testdata/ca-ia.pem",
				SSLClientCertificateKeyFileSet: true,
				SSLClientCertificateKeyFile:    "../db/testdata/test-client.pem",
			},
		}
	}

	return &options.URI{}
}

func TestNewSessionProvider(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	auth := DBGetAuthOptions()
	ssl := DBGetSSLOptions()
	Convey("When initializing a session provider", t, func() {

		Convey("with the standard options, a provider with a standard"+
			" connector should be returned", func() {
			opts := options.ToolOptions{
				Connection: &options.Connection{
					Port: DefaultTestPort,
				},
				URI:  DBGetConnString(),
				SSL:  &ssl,
				Auth: &auth,
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
				URI:  DBGetConnString(),
				SSL:  &ssl,
				Auth: &auth,
			}
			provider, err := NewSessionProvider(opts)
			So(err, ShouldBeNil)
			So(provider.client.Ping(context.Background(), nil), ShouldBeNil)
		})

	})
}

func TestConfigureClientForSRV(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	Convey("Configuring options with a URI with invalid auth should succeed", t, func() {
		enabled := options.EnabledOptions{
			Auth:       true,
			Connection: true,
			Namespace:  true,
			URI:        true,
		}

		toolOptions := options.New("test", "", "", "", true, enabled)
		// AuthSource without a username is invalid, we want to check the URI does not get
		// validated as part of client configuration
		_, err := toolOptions.ParseArgs([]string{"--uri", "mongodb://foo/?authSource=admin", "--username", "bar"})
		So(err, ShouldBeNil)

		_, err = configureClient(*toolOptions)
		So(err, ShouldBeNil)
	})

}

func TestDatabaseNames(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	auth := DBGetAuthOptions()
	ssl := DBGetSSLOptions()

	Convey("With a valid session provider", t, func() {
		opts := options.ToolOptions{
			Connection: &options.Connection{
				Port: DefaultTestPort,
			},
			URI:  DBGetConnString(),
			SSL:  &ssl,
			Auth: &auth,
		}
		provider, err := NewSessionProvider(opts)
		So(err, ShouldBeNil)

		err = provider.DropDatabase("exists")
		So(err, ShouldBeNil)
		err = provider.CreateCollection("exists", "collection")
		So(err, ShouldBeNil)
		err = provider.DropDatabase("missingDB")
		So(err, ShouldBeNil)

		Convey("When DatabaseNames is called", func() {
			names, err := provider.DatabaseNames()
			So(err, ShouldBeNil)
			So(len(names), ShouldBeGreaterThan, 0)

			m := make(map[string]bool)
			for _, v := range names {
				m[v] = true
			}

			So(m["exists"], ShouldBeTrue)
			So(m["misssingDB"], ShouldBeFalse)
		})
	})
}

func TestFindOne(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	auth := DBGetAuthOptions()
	ssl := DBGetSSLOptions()

	Convey("With a valid session provider", t, func() {
		opts := options.ToolOptions{
			Connection: &options.Connection{
				Port: DefaultTestPort,
			},
			URI:  DBGetConnString(),
			SSL:  &ssl,
			Auth: &auth,
		}
		provider, err := NewSessionProvider(opts)
		So(err, ShouldBeNil)

		err = provider.DropDatabase("exists")
		So(err, ShouldBeNil)
		err = provider.CreateCollection("exists", "collection")
		So(err, ShouldBeNil)
		client, err := provider.GetSession()
		So(err, ShouldBeNil)
		coll := client.Database("exists").Collection("collection")
		coll.InsertOne(context.Background(), bson.D{})

		Convey("When FindOneis called", func() {
			res := bson.D{}
			err := provider.FindOne("exists", "collection", 0, nil, nil, &res, 0)
			So(err, ShouldBeNil)
		})
	})
}

func TestGetIndexes(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	auth := DBGetAuthOptions()
	ssl := DBGetSSLOptions()
	Convey("With a valid session", t, func() {
		opts := options.ToolOptions{
			Connection: &options.Connection{
				Port: DefaultTestPort,
			},
			URI:  DBGetConnString(),
			SSL:  &ssl,
			Auth: &auth,
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
					ctx := context.Background()
					counter := 0
					for indexesIter.Next(ctx) {
						counter++
					}
					So(counter, ShouldBeGreaterThan, 0)
				})
			})

			Convey("a missing collection there should be no error", func() {
				indexesIter, err := GetIndexes(missing)
				So(err, ShouldBeNil)
				Convey("and there should be no indexes", func() {
					So(indexesIter.Next(nil), ShouldBeFalse)
				})
			})

			Convey("a missing database there should be no error", func() {
				indexesIter, err := GetIndexes(missingDB)
				So(err, ShouldBeNil)
				Convey("and there should be no indexes", func() {
					So(indexesIter.Next(nil), ShouldBeFalse)
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

func (*listDatabasesCommand) AsRunnable() interface{} {
	return "listDatabases"
}

func TestServerVersionArray(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)

	auth := DBGetAuthOptions()
	ssl := DBGetSSLOptions()

	Convey("With a valid session provider", t, func() {
		opts := options.ToolOptions{
			Connection: &options.Connection{
				Port: DefaultTestPort,
				Host: "localhost",
			},
			URI:  DBGetConnString(),
			SSL:  &ssl,
			Auth: &auth,
		}
		provider, err := NewSessionProvider(opts)
		So(err, ShouldBeNil)

		version, err := provider.ServerVersionArray()
		So(err, ShouldBeNil)
		So(version.GT(Version{}), ShouldBeTrue)
	})
}

func TestServerCertificateVerification(t *testing.T) {
	testtype.SkipUnlessTestType(t, testtype.IntegrationTestType)
	testtype.SkipUnlessTestType(t, testtype.SSLTestType)

	auth := DBGetAuthOptions()
	sslOrigin := DBGetSSLOptions()
	Convey("When initializing a session provider", t, func() {
		Convey("connection shall succeed if provided with intermediate certificate only as well", func() {
			ssl := sslOrigin
			ssl.SSLCAFile = "../db/testdata/ia.pem"
			opts := options.ToolOptions{
				Connection: &options.Connection{
					Port:    DefaultTestPort,
					Timeout: 10,
				},
				URI:  DBGetConnString(),
				SSL:  &ssl,
				Auth: &auth,
			}
			opts.URI.ConnString.SSLCaFile = "../db/testdata/ia.pem"
			provider, err := NewSessionProvider(opts)
			So(err, ShouldBeNil)
			So(provider.client.Ping(context.Background(), nil), ShouldBeNil)
			Convey("and should be closeable", func() {
				provider.Close()
			})
		})
	})
}

