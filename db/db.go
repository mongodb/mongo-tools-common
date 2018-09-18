// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package db implements generic connection to MongoDB, and contains
// subpackages for specific methods of connection.
package db

import (
	"context"
	"errors"
	"time"

	"github.com/mongodb/mongo-go-driver/core/readpref"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"github.com/mongodb/mongo-go-driver/mongo/dbopt"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/password"
	"gopkg.in/mgo.v2/bson"

	"fmt"
	"io"
	"strings"
	"sync"
)

type (
	sessionFlag uint32
)

// Session flags.
const (
	None      sessionFlag = 0
	Monotonic sessionFlag = 1 << iota
	DisableSocketTimeout
)

// MongoDB enforced limits.
const (
	MaxBSONSize = 16 * 1024 * 1024 // 16MB - maximum BSON document size
)

// Default port for integration tests
const (
	DefaultTestPort = "33333"
)

const (
	ErrLostConnection     = "lost connection to server"
	ErrNoReachableServers = "no reachable servers"
	ErrNsNotFound         = "ns not found"
	// replication errors list the replset name if we are talking to a mongos,
	// so we can only check for this universal prefix
	ErrReplTimeoutPrefix            = "waiting for replication timed out"
	ErrCouldNotContactPrimaryPrefix = "could not contact primary for replica set"
	ErrWriteResultsUnavailable      = "write results unavailable from"
	ErrCouldNotFindPrimaryPrefix    = `could not find host matching read preference { mode: "primary"`
	ErrUnableToTargetPrefix         = "unable to target"
	ErrNotMaster                    = "not master"
	ErrConnectionRefusedSuffix      = "Connection refused"
)

// Used to manage database sessions
type SessionProvider struct {
	sync.Mutex

	// the master client used for operations
	client *mongo.Client

	// whether Connect has been called on the mongoClient
	connectCalled bool

	// default read preference for new database objects
	readPref *readpref.ReadPref
}

// ApplyOpsResponse represents the response from an 'applyOps' command.
type ApplyOpsResponse struct {
	Ok     bool   `bson:"ok"`
	ErrMsg string `bson:"errmsg"`
}

// Oplog represents a MongoDB oplog document.
type Oplog struct {
	Timestamp bson.MongoTimestamp `bson:"ts"`
	HistoryID int64               `bson:"h"`
	Version   int                 `bson:"v"`
	Operation string              `bson:"op"`
	Namespace string              `bson:"ns"`
	Object    bson.D              `bson:"o"`
	Query     bson.D              `bson:"o2"`
	UI        *bson.Binary        `bson:"ui,omitempty"`
}

// Returns a mongo.Client connected to the database server for which the
// session provider is configured.
func (self *SessionProvider) GetSession() (*mongo.Client, error) {
	self.Lock()
	defer self.Unlock()

	if self.client == nil {
		return nil, errors.New("SessionProvider already closed")
	}

	if !self.connectCalled {
		self.client.Connect(context.Background())
		self.connectCalled = true
	}

	return self.client, nil
}

// Close closes the master session in the connection pool
func (self *SessionProvider) Close() {
	self.Lock()
	defer self.Unlock()
	if self.client != nil {
		self.client.Disconnect(context.Background())
		self.client = nil
	}
}

// DB provides a database with the default read preference
func (self *SessionProvider) DB(name string) *mongo.Database {
	self.Lock()
	defer self.Unlock()
	return self.client.Database(name, dbopt.ReadPreference(self.readPref))
}

// SetFlags allows certain modifications to the masterSession after initial creation.
func (self *SessionProvider) SetFlags(flagBits sessionFlag) {
	panic("unsupported")
}

// SetReadPreference sets the read preference mode in the SessionProvider
// and eventually in the masterSession
func (self *SessionProvider) SetReadPreference(pref *readpref.ReadPref) {
	self.Lock()
	defer self.Unlock()
	self.readPref = pref
}

// SetBypassDocumentValidation sets whether to bypass document validation in the SessionProvider
// and eventually in the masterSession
func (self *SessionProvider) SetBypassDocumentValidation(bypassDocumentValidation bool) {
	panic("unsupported")
}

// SetTags sets the server selection tags in the SessionProvider
// and eventually in the masterSession
func (self *SessionProvider) SetTags(tags bson.D) {
	panic("unsupported")
}

// NewSessionProvider constructs a session provider but does not attempt to
// create the initial session.
func NewSessionProvider(opts options.ToolOptions) (*SessionProvider, error) {
	// finalize auth options, filling in missing passwords
	if opts.Auth.ShouldAskForPassword() {
		opts.Auth.Password = password.Prompt()
	}

	client, err := configureClient(opts)
	if err != nil {
		return nil, fmt.Errorf("error configuring the connector: %v", err)
	}

	// create the provider
	return &SessionProvider{client: client}, nil
}

func configureClient(opts options.ToolOptions) (*mongo.Client, error) {
	clientOpts := make([]clientopt.Option, 0)
	timeout := time.Duration(opts.Timeout) * time.Second

	clientOpts = append(
		clientOpts,
		clientopt.ConnectTimeout(timeout),
		clientopt.ReplicaSet(opts.ReplicaSetName),
		clientopt.Single(opts.Direct),
	)

	if opts.Auth != nil {
		auth := clientopt.Credential{
			Username:      opts.Auth.Username,
			Password:      opts.Auth.Password,
			AuthSource:    opts.GetAuthenticationDatabase(),
			AuthMechanism: opts.Auth.Mechanism,
		}
		if opts.Kerberos != nil && auth.AuthMechanism == "GSSAPI" {
			props := make(map[string]string)
			if opts.Kerberos.Service != "" {
				props["SERVICE_NAME"] = opts.Kerberos.Service
			}
			// XXX How do we use opts.Kerberos.ServiceHost if at all?
			auth.AuthMechanismProperties = props
		}
		clientOpts = append(clientOpts, clientopt.Auth(auth))
	}

	if opts.SSL != nil {
		// Error on unsupported features
		if opts.SSLFipsMode {
			return nil, fmt.Errorf("FIPS mode not supported")
		}
		if opts.SSLCRLFile != "" {
			return nil, fmt.Errorf("CRL files are not supported on this platform")
		}

		ssl := &clientopt.SSLOpt{Enabled: opts.UseSSL}
		if opts.SSLAllowInvalidCert || opts.SSLAllowInvalidHost {
			ssl.Insecure = true
		}
		if opts.SSLPEMKeyFile != "" {
			ssl.ClientCertificateKeyFile = opts.SSLPEMKeyFile
			if opts.SSLPEMKeyPassword != "" {
				ssl.ClientCertificateKeyPassword = func() string { return opts.SSLPEMKeyPassword }
			}
		}
		if opts.SSLCAFile != "" {
			ssl.CaFile = opts.SSLCAFile
		}
		clientOpts = append(clientOpts, clientopt.SSL(ssl))
	}

	var uri string
	if opts.URI != nil && opts.URI.ConnectionString != "" {
		uri = opts.URI.ConnectionString
	} else {
		host := opts.Host
		if host == "" {
			host = "localhost"
		}
		port := opts.Port
		if port == "" {
			port = "27017"
		}

		uri = fmt.Sprintf("mongodb://%s:%s/", host, port)
	}

	return mongo.NewClientWithOptions(uri, clientOpts...)
}

// IsConnectionError returns a boolean indicating if a given error is due to
// an error in an underlying DB connection (as opposed to some other write
// failure such as a duplicate key error)
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	lowerCaseError := strings.ToLower(err.Error())
	if lowerCaseError == ErrNoReachableServers ||
		err == io.EOF ||
		strings.Contains(lowerCaseError, ErrReplTimeoutPrefix) ||
		strings.Contains(lowerCaseError, ErrCouldNotContactPrimaryPrefix) ||
		strings.Contains(lowerCaseError, ErrWriteResultsUnavailable) ||
		strings.Contains(lowerCaseError, ErrCouldNotFindPrimaryPrefix) ||
		strings.Contains(lowerCaseError, ErrUnableToTargetPrefix) ||
		lowerCaseError == ErrNotMaster ||
		strings.HasSuffix(lowerCaseError, ErrConnectionRefusedSuffix) {
		return true
	}
	return false
}
