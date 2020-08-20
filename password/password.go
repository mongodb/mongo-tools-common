// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package password handles cleanly reading in a user's password from
// the command line. This varies heavily between operating systems.
package password

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/mongodb/mongo-tools-common/log"
)

// key constants
const (
	backspaceKey      = 8
	deleteKey         = 127
	eotKey            = 3
	eofKey            = 4
	newLineKey        = 10
	carriageReturnKey = 13
)

// Prompt displays a prompt asking for the password and returns the
// password the user enters as a string.
func Prompt() (string, error) {
	var pass string
	var err error
	if IsTerminal() {
		log.Logv(log.DebugLow, "standard input is a terminal; reading password from terminal")
		fmt.Fprintf(os.Stderr, "Enter password:")
		pass, err = readPassInteractively(os.Stdin)
	} else {
		log.Logv(log.Always, "reading password from standard input")
		fmt.Fprintf(os.Stderr, "Enter password:")
		pass, err = readPassNonInteractively(os.Stdin)
	}
	if err != nil {
		return "", err
	}
	fmt.Fprintln(os.Stderr)
	return pass, nil
}

// readPassNonInteractively pipes in a password from stdin if
// we aren't using a terminal for standard input
func readPassNonInteractively(reader io.Reader) (string, error) {
	pass := []byte{}

	chBuf, err := ioutil.ReadAll(reader)
	if err != nil && err.Error() != "EOF" {
		return "", err
	}

	for _, ch := range chBuf {
		if ch == backspaceKey || ch == deleteKey {
			if len(pass) > 0 {
				pass = pass[:len(pass)-1]
			}
		} else if ch == carriageReturnKey || ch == newLineKey || ch == eotKey || ch == eofKey {
			break
		} else if ch != 0 {
			pass = append(pass, ch)
		}
	}
	return string(pass), nil
}
