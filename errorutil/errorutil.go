package errorutil

import (
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

type WriteConcernError struct {
	Code     int    `bson:"code"`
	CodeName string `bson:"codeName"`
	Message  string `bson:"errmsg"`
}

func (err *WriteConcernError) Error() string {
	if err.CodeName == "" {
		return fmt.Sprintf("WriteConcernError: %s, code: %v", err.Message, err.Code)
	}
	return fmt.Sprintf("WriteConcernError: %s, code: %v, codeName: %v", err.Message, err.Code, err.CodeName)
}

// isCursorNotFoundError checks if err is an "CursorNotFound" error.
func isCursorNotFoundError(err error) bool {
	code := getErrorCode(err)
	return code == 43 || strings.Contains(err.Error(), "cursor not found")
}

// isAuthError checks if err is an "Unauthorized" error.
func isAuthError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 13
}

func isDuplicateKeyCode(code int) bool {
	return code == 11000 || code == 11001 || code == 12582
}

// IsDuplicateKeyError checks if err is a "DuplicateKey" error.
func IsDuplicateKeyError(err error) bool {
	if cmdErr, ok := err.(mongo.CommandError); ok {
		return isDuplicateKeyCode(int(cmdErr.Code))
	}
	if writeException, ok := err.(mongo.WriteException); ok {
		for _, we := range writeException.WriteErrors {
			if isDuplicateKeyCode(we.Code) {
				return true
			}
		}
	}
	if bwe, ok := err.(mongo.BulkWriteException); ok {
		for _, we := range bwe.WriteErrors {
			if isDuplicateKeyCode(we.Code) {
				return true
			}
		}
	}
	return false
}

// isNoNamespaceError checks if err is an "NamespaceNotFound" error.
func isNoNamespaceError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 26
}

// isNamespaceExistsError checks if err is an "NamespaceExists" error.
func isNamespaceExistsError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 48
}

// IsCommandNotFound checks if err is an "CommandNotFound" error.
func IsCommandNotFound(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && (e.Code == 59 || e.Code == 13390 || strings.Contains(e.Message, "no such cmd"))
}

// IsUserNotFound checks if err is a "UserNotFound" error
func IsUserNotFound(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 11
}

// isViewError checks if err is an "CommandNotSupportedOnView" error.
func isViewError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 166
}

// isOptionsError checks if err is an "InvalidOptions" error.
func isOptionsError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 72
}

// isBadHintError checks if err is an "bad hint" error.
func isBadHintError(err error) bool {
	e, ok := err.(mongo.CommandError)
	if ok {
		if e.Code == 17007 {
			// MongoDB <= 3.0 returns: {
			//	"$err" : "Unable to execute query: error processing query: ...\n planner returned error: bad hint",
			//	"code" : 17007
			// }
			return true
		}
		if e.Code == 2 && strings.Contains(e.Error(), "bad hint") {
			// MongoDB >= 3.2 returns: {
			//	"$err" : "error processing query: ...\n planner returned error: bad hint",
			//	"code" : 2
			// }
			return true
		}
	}
	return false
}

// IsInvalidIndexSpecificationOptionError checks if err is an "InvalidIndexSpecificationOption" error.
func IsInvalidIndexSpecificationOptionError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 197
}

// IsCannotCreateIndexError checks if err is a "CannotCreateIndex" error.
func IsCannotCreateIndexError(err error) bool {
	e, ok := err.(mongo.CommandError)
	return ok && e.Code == 67
}

// isNetworkError checks if err is a network error.
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// Connection errors from syscalls, connection reset by peer, etc..
	if _, ok := err.(net.Error); ok {
		return true
	}
	// Connection errors from mgo.
	if err == io.EOF || err.Error() == "no reachable servers" || err.Error() == "Closed explicitly" {
		return true
	}
	// Connection errors from spacemonkeygo/openssl.
	if err == io.ErrUnexpectedEOF || err.Error() == "connection closed" {
		return true
	}
	// Network errors from the driver
	if ce, ok := err.(mongo.CommandError); ok {
		return ce.HasErrorLabel("NetworkError")
	}
	return false
}

func getErrorCode(err error) int {
	switch e := err.(type) {
	case mongo.CommandError:
		return int(e.Code)
	case driver.Error:
		return int(e.Code)
	case driver.WriteCommandError:
		for _, we := range e.WriteErrors {
			return int(we.Code)
		}
		if e.WriteConcernError != nil {
			return int(e.WriteConcernError.Code)
		}
		return 0
	case driver.QueryFailureError:
		codeVal, err := e.Response.LookupErr("code")
		if err == nil {
			code, _ := codeVal.Int32OK()
			return int(code)
		}
		return 0 // this shouldn't happen
	case mongo.WriteError:
		return e.Code
	case mongo.BulkWriteError:
		return e.Code
	case mongo.WriteConcernError:
		return e.Code
	case mongo.WriteException:
		for _, we := range e.WriteErrors {
			return getErrorCode(we)
		}
		if e.WriteConcernError != nil {
			return e.WriteConcernError.Code
		}
		return 0
	case mongo.BulkWriteException:
		// Return the first error code.
		for _, ecase := range e.WriteErrors {
			return getErrorCode(ecase)
		}
		if e.WriteConcernError != nil {
			return e.WriteConcernError.Code
		}
		return 0
	default:
		return 0
	}
}

// isReconnectableError checks if we can reconnect to the cluster.
func IsReconnectableError(err error) bool {
	// Find the root cause.
	err = errors.Cause(err)
	if err == nil {
		return false
	}

	// All w:majority write concern errors are retryable.
	if _, ok := err.(*WriteConcernError); ok {
		return true
	}

	switch getErrorCode(err) {
	case 10107, 13435, 13436, 64, 6, 7, 89, 9001, 91, 189, 11600, 11601, 11602, 136:
		// These error codes are either listed as retryable in
		// the remote command retry scheduler, or have been
		// added here deliberately, since they have been
		// observed to be issued when applyOps/find/getMore is
		// interrupted while the server is being shut down.
		//
		// 10107        NotMaster
		// 13435        NotMasterNoSlaveOk
		// 13436        NotMasterOrSecondary
		// 64           WriteConcernFailed
		// 6            HostUnreachable
		// 7            HostNotFound
		// 89           NetworkTimeout
		// 9001         SocketException
		// 91           ShutdownInProgress
		// 189          PrimarySteppedDown
		// 11600        InterruptedAtShutdown
		// 11601        Interrupted
		// 11602        InterruptedDueToReplStateChange
		// 136          CappedPositionLost
		return true
	case 175:
		// 175          QueryPlanKilled, for example:
		// "PlanExecutor killed: InterruptedDueToReplStateChange: operation was interrupted"
		// This is only an issue on 3.6.0-3.6.3, see SPEC-1059.
		return true
	case 0:
		// The server may send "not master" without an error code.
		if strings.Contains(err.Error(), "not master") {
			return true
		}
	}

	if isNetworkError(err) {
		// Retry on network errors, eg no reachable servers, connection reset
		// by peer, operation timed out, etc...
		return true
	}
	return false
}
