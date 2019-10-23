package db

import (
	"context"
	"fmt"
	"time"

	"github.com/mongodb/mongo-tools-common/bsonutil"
	"github.com/mongodb/mongo-tools-common/errorutil"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// const (
// 	DuplicateKeyErrorCode int32 = 11000
// )

const (
	// ConnectionRetriesLowerBound is the least number of attempts to re-establish a connection.
	ConnectionRetriesLowerBound = 10
	// CommandRetriesLowerBound is the least number of command retry attempts.
	CommandRetriesLowerBound = 10
	// RetryDurationLowerBound is the least amount of time to wait for all retry attempts.
	RetryDurationLowerBound = 5 * time.Minute
	// RecoverSleepDuration is the time to sleep before attempting to recover a session.
	RecoverSleepDuration = 5 * time.Second
)

type writeResult struct {
	ConcernError *WriteConcernError `bson:"writeConcernError"`
}

// // BuildInfo represents a response for the buildInfo command.
type BuildInfo struct {
	Version        string
	VersionArray   []int  `bson:"versionArray"` // On MongoDB 2.0+; assembled from Version otherwise
	GitVersion     string `bson:"gitVersion"`
	OpenSSLVersion string `bson:"OpenSSLVersion"`
	SysInfo        string `bson:"sysInfo"` // Deprecated and empty on MongoDB 3.2+.
	Bits           int
	Debug          bool
	MaxObjectSize  int `bson:"maxBsonObjectSize"`
}

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

// VersionAtLeast returns whether the BuildInfo version is greater than or
// equal to the provided version number. If more than one number is
// provided, numbers will be considered as major, minor, and so on.
func (bi *BuildInfo) VersionAtLeast(version ...int) bool {
	for i, vi := range version {
		if i == len(bi.VersionArray) {
			return false
		}
		if bivi := bi.VersionArray[i]; bivi != vi {
			return bivi >= vi
		}
	}
	return true
}

// // FullCollectionName returns the full namespace for a collection (dbName.collectionName)
func FullCollectionName(c *mongo.Collection) string {
	return fmt.Sprintf("%s.%s", c.Database().Name(), c.Name())
}

// DbCommandRunner wraps a *mongo.Database and implements the CommandRunner interface
// This type was created to minimize diff noise while porting mongomirror to the new driver.
// TODO(MGOMIRROR-241) Remove this type
type DbCommandRunner struct {
	*mongo.Database
}

func (dbcr *DbCommandRunner) Run(cmd interface{}, result interface{}) error {
	res := dbcr.RunCommand(context.Background(), cmd)
	if err := res.Err(); err != nil {
		return err
	}

	return res.Decode(result)
}

// RunCommandWithLog runs the given command with some logging.
func RunCommandWithLog(d *mongo.Database, cmd bson.D, result interface{}) error {
	cmdName := cmd[0].Key
	log.Logvf(log.DebugLow, "Running %s on database: `%v`", cmdName, d.Name())
	start := time.Now()
	err := runCheckWriteConcernError(&DbCommandRunner{d}, cmd, result)
	if err != nil {
		log.Logvf(log.Always, "%s on database: `%v`, finished in %s with error: %v",
			cmdName, d.Name(), time.Since(start), err)
		return err
	}
	log.Logvf(log.DebugLow, "%s on database: `%v`, finished in %s",
		cmdName, d.Name(), time.Since(start))
	return nil
}

// RunRetryableFunc runs the given function and retries after a network error.
func RunRetryableFunc(s *mongo.Client, f func(isRetry bool) error) error {
	err := f(false)
	if err == nil {
		return nil
	}
	start := time.Now()
	i := 0
	for ; i < CommandRetriesLowerBound || time.Since(start) < RetryDurationLowerBound; i++ {
		if !errorutil.IsReconnectableError(err) {
			log.Logvf(log.Info, "Error on destination: %#v", err)
			return err
		}
		log.Logvf(log.Always, "Reconnecting to the destination after transient error: %#v", err)
		err = RecoverSession(start, s, "destination", true)
		if err != nil {
			return err
		}
		err = f(true)
		if err == nil {
			return nil
		}
	}
	return errors.Wrapf(err, "gave up retrying after %v failed attempts which took %s", i+1, time.Since(start))
}

// RunRetryableInsert inserts the given documents into the collection and
// retries after a network error.
func RunRetryableInsert(c *mongo.Collection, docs []interface{}, opts ...*options.InsertManyOptions) error {
	for len(docs) > 0 {
		err := RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
			_, err := c.InsertMany(context.Background(), docs, opts...)
			return err
		})
		if !errorutil.IsDuplicateKeyError(err) {
			return err
		}
		// After duplicate key error, insert one at a time until the
		// first success. Then we can bulk insert the remaining docs.
		// TODO: This may be very slow. Some faster approaches might be:
		// - count by query to get all the docs on the destination
		//   with these _ids
		// - run a find on the destination oplog for this collection
		//   to get the last inserted document.

		// translate InsertManyOptions into InsertOneOptions
		insertOneOpts := options.InsertOne()
		for _, opt := range opts {
			if opt.BypassDocumentValidation != nil {
				insertOneOpts.SetBypassDocumentValidation(*opt.BypassDocumentValidation)
			}
		}

		i := 0
		for i < len(docs) {
			err := RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
				_, err := c.InsertOne(context.Background(), docs[i], insertOneOpts)
				return err
			})
			i++
			if err == nil {
				break
			} else if errorutil.IsDuplicateKeyError(err) {
				continue
			} else {
				return err
			}
		}
		docs = docs[i:]
	}
	return nil
}

// RunRetryableCreate runs a create collection command and retries after a
// network error.
func RunRetryableCreate(db *mongo.Database, createCmd bson.D) error {
	return RunRetryableFunc(db.Client(), func(isRetry bool) error {
		err := RunCommandWithLog(db, withWMajority(createCmd), nil)
		// Ignore when the namespace already exists on a retry attempt.
		if isRetry && errorutil.IsNamespaceExistsError(err) {
			return nil
		}
		return err
	})
}

// RunRetryableRenameAndDrop renames and drops the given collection,
// retrying after network errors.
func RunRetryableRenameAndDrop(c *mongo.Collection) error {
	newColl := c.Database().Collection(fmt.Sprintf("_mongomirror_drop_pending_%s", c.Name()))
	cmd := bson.D{
		{"renameCollection", FullCollectionName(c)},
		{"to", FullCollectionName(newColl)},
	}
	err := RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
		err := RunCommandWithLog(c.Database().Client().Database("admin"), withWMajority(cmd), nil)
		// Ignore when the namespace is not found on a retry attempt.
		if isRetry && errorutil.IsNoNamespaceError(err) {
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return RunRetryableDrop(newColl)
}

// RunRetryableDrop drops the given collection and retries after a network error.
func RunRetryableDrop(c *mongo.Collection) error {
	if c.Name() == "system.js" {
		// The server does not let you drop "system.js" collections unless
		// you say the magic words, SERVER-5972.
		return RunRetryableRenameAndDrop(c)
	}
	return RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
		err := RunCommandWithLog(c.Database(), withWMajority(bson.D{{"drop", c.Name()}}), nil)
		// Ignore when the namespace is not found on a retry attempt.
		if isRetry && errorutil.IsNoNamespaceError(err) {
			return nil
		}
		return err
	})
}

// RunRetryableDropDatabase drops the given database and retries after a network error.
func RunRetryableDropDatabase(db *mongo.Database) error {
	return RunRetryableFunc(db.Client(), func(isRetry bool) error {
		return RunCommandWithLog(db, withWMajority(bson.D{{"dropDatabase", 1}}), nil)
	})
}

// RunRetryableCollMod runs a collMod command to modify a view definition
// and retries after a network error.  On 3.6+, collMod supports write concern,
// otherwise, we wait for a no-op applyOps.
func RunRetryableCollMod(c *mongo.Collection, collModCmd bson.D, destInfo *BuildInfo) error {
	return RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
		if destInfo.VersionAtLeast(3, 6, 0) {
			return RunCommandWithLog(c.Database(), withWMajority(collModCmd), nil)
		}

		err := RunCommandWithLog(c.Database(), collModCmd, nil)
		if err != nil {
			return err
		}
		return WaitForWriteConcernMajority(c.Database().Client())
	})
}

// ApplyOpsErrorToLog returns an error to log given an applyOps response and
// an mgo error. The response contains more information than the mgo error
// so return that if it exists.
func ApplyOpsErrorToLog(res *ApplyOpsResponse, err error) error {
	if res == nil {
		return err
	}
	byteValue, err := bson.MarshalExtJSON(res, true, false)
	if err != nil {
		return errors.Errorf("%+v", res)
	}
	return errors.Errorf("%s", byteValue)
}

// RunRetryableApplyOps runs applyOps with a batch of oplog entries and
// retries after a network error.
func RunRetryableApplyOps(s *mongo.Client, entries []bson.Raw, bytes int, bypassValidation bool) (res *ApplyOpsResponse, err error) {
	err = RunRetryableFunc(s, func(isRetry bool) error {
		var retry string
		if isRetry {
			retry = "retry "
		} else {
			retry = ""
		}

		start := time.Now()
		res, err = applyOpsBatchBypassValidation(&DbCommandRunner{Database: s.Database("admin")}, entries, bypassValidation)
		end := time.Since(start)

		if err == nil {
			log.Logvf(log.DebugLow, "applyOps %swith %d operation(s) (%d bytes) succeeded after %s.",
				retry, len(entries), bytes, end)
		} else {
			// The applyOps response contains more information than the mgo error.
			log.Logvf(log.Info, "applyOps %swith %d operation(s) (%d bytes) failed after %s: %s",
				retry, len(entries), bytes, end, ApplyOpsErrorToLog(res, err))
		}

		return err
	})
	return res, err
}

// RunRetryableCreateIndexes runs a createIndexes command and retries after
// a network error.
// NOTE: mgo will return an error if the session's socket fails in between
// the createIndexes and the applyOps.
func RunRetryableCreateIndexes(c *mongo.Collection, indexes []bson.D, destInfo *BuildInfo) error {
	for i, index := range indexes {
		indexes[i] = FixOutgoingIndexSpec(index)
	}
	return RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
		log.Logvf(log.DebugLow, "Running createIndexes for collection: `%v`, indexes: %v",
			FullCollectionName(c), indexes)
		start := time.Now()
		err := createIndexes(&DbCommandRunner{c.Database()}, c.Name(), indexes)
		if err != nil {
			log.Logvf(log.Always, "createIndexes for collection: `%v`, finished in %s with error: %v",
				FullCollectionName(c), time.Since(start), err)
			return err
		}
		log.Logvf(log.Always, "createIndexes for collection: `%v`, finished in %s",
			FullCollectionName(c), time.Since(start))

		// If the destination does not support write concern for createIndexes we
		// run a no-op applyOps command with majority write concern to wait for
		// the index builds on secondaries to complete.
		// SERVER-20224 added write concern support to createIndexes in 3.3.5.
		if !destInfo.VersionAtLeast(3, 3, 5) {
			log.Logv(log.Always, "Waiting for index builds on a majority of nodes to complete.")
			err = WaitForWriteConcernMajority(c.Database().Client())
			if err != nil {
				return errors.Wrapf(err, "error waiting for index builds on a majority of nodes to complete")
			}
		}
		return nil
	})
}

func FixOutgoingIndexSpec(index bson.D) bson.D {
	RemoveKey("background", &index)
	// Oplog entries before 3.4 do not contain the index version field.
	return AppendV1IfMissing(index)
}

func AppendV1IfMissing(index bson.D) bson.D {
	_, err := bsonutil.FindValueByKey("v", &index)
	if err == nil {
		return index
	}
	return append(index, bson.E{"v", 1})
}

// RunRetryableApplyOpsCreateIndex creates the given `index` with an applyOps command and retries after
// network errors.
// `uuid` must be the UUID of the target collection `c`, or nil if the target collection has no uuid.
func RunRetryableApplyOpsCreateIndex(c *mongo.Collection, index bson.D, uuid *primitive.Binary) (*ApplyOpsResponse, error) {
	index = FixOutgoingIndexSpec(index)
	createOplog := Oplog{}
	if uuid == nil {
		// Destination does not support UUIDs, insert into system.indexes.
		// NOTE: this will fail until SERVER-36944 is implemented.
		log.Logvf(log.Always, "Running system.indexes applyOps to create index on collection: `%v`, index: %v",
			FullCollectionName(c), index)
		createOplog.Operation = "i"
		createOplog.Namespace = fmt.Sprintf("%s.system.indexes", c.Database().Name())
		createOplog.Object = index

	} else {
		// Destination supports UUIDs, use the createIndexes oplog command.
		log.Logvf(log.Always, "Running createIndexes with applyOps to create index on collection: `%v`, index: %v",
			FullCollectionName(c), index)
		createOplog.Operation = "c"
		createOplog.Namespace = fmt.Sprintf("%s.$cmd", c.Database().Name())
		createOplog.UI = uuid
		createOplog.Object = append(bson.D{{"createIndexes", c.Name()}}, index...)
	}
	rawCreateOplog, err := bson.Marshal(createOplog)
	if err != nil {
		return nil, err
	}
	return RunRetryableApplyOps(c.Database().Client(), []bson.Raw{rawCreateOplog}, len(rawCreateOplog), false)
}

// RunRetryableCreateIndexesWithFallback creates all the given `indexes` and retries after
// network errors. If the initial createIndexes command fails due to a CannotCreateIndex
// or InvalidIndexSpecificationOption error, we fallback to using applyOps on each index individually.
// `uuid` must be the UUID of the target collection `c`, or nil if the target collection has no uuid.
func RunRetryableCreateIndexesWithFallback(c *mongo.Collection, indexes []bson.D, destInfo *BuildInfo, uuid *primitive.Binary) error {
	err := RunRetryableCreateIndexes(c, indexes, destInfo)
	if err == nil {
		return nil
	}
	if !(errorutil.IsInvalidIndexSpecificationOptionError(err) || errorutil.IsCannotCreateIndexError(err)) {
		return err
	}
	log.Logvf(log.Always, "createIndexes for collection `%v` failed: %v. retrying each index build individually",
		FullCollectionName(c), err.Error())

	for _, index := range indexes {
		_, err := RunRetryableApplyOpsCreateIndex(c, index, uuid)
		if err != nil {
			return err
		}
	}
	return nil
}

// RunRetryableCollectionInfo runs GetCollectionInfo (listCollections)
// and retries after network errors.
func RunRetryableCollectionInfo(c *mongo.Collection) (collInfo *CollectionInfo, err error) {
	err = RunRetryableFunc(c.Database().Client(), func(isRetry bool) error {
		collInfo, err = GetCollectionInfo(c)
		return err
	})
	return
}

// To ensure w:majority on the destination we need to first perform a
// w:majority no-op applyOps to "flush" the destination cluster.
// This ensures that a retry attempt is based on a majority commit
// after the failed attempt. As a concrete example, imagine the sequence:
// first attempt: insert({_id: 1}) -> Network error
// retry attempt: insert({_id: 1}) -> Duplicate key error
// After the duplicate key error, the document is not guaranteed to be
// majority committed.
func RecoverSession(start time.Time, session *mongo.Client, description string, writeable bool) error {
	isMaster := bson.M{"isMaster": 1}
	var err error
	i := 0
	for ; i < ConnectionRetriesLowerBound || time.Since(start) < RetryDurationLowerBound; i++ {
		time.Sleep(RecoverSleepDuration)
		if writeable {
			err = WaitForWriteConcernMajority(session)
		} else {
			err = session.Database("admin").RunCommand(context.Background(), isMaster).Err()
		}
		if err == nil {
			// Reconnected.
			return nil
		}
		if errorutil.IsReconnectableError(err) {
			log.Logvf(log.Always, "Reconnection attempt %v failed: %#v", i+1, err)
			continue
		} else {
			// Unrecoverable error.
			log.Logvf(log.Always, "Reconnection attempt %v failed with unrecoverable error: %#v",
				i+1, err)
			break
		}
	}
	return errors.Wrapf(err, "gave up reconnecting to the %v after %v "+
		"failed attempts which took %s", description, i, time.Since(start))
}

// runWithWriteConcernMajority runs a command with w:majority and and checks
// for write concern errors.
func runWithWriteConcernMajority(c CommandRunner, cmd bson.D, res interface{}) error {
	return runCheckWriteConcernError(c, withWMajority(cmd), res)
}

// runCheckWriteConcernError runs a command checks for write concern errors.
func runCheckWriteConcernError(c CommandRunner, cmd bson.D, res interface{}) error {
	raw := bson.Raw{}
	cmdErr := c.Run(cmd, &raw)
	// Unmarshal the final type, even on error.
	if res != nil && len(raw) > 0 {
		err := bson.Unmarshal(raw, res)
		if err != nil {
			return errors.Errorf("failed to unmarshal bson.Raw into res: %v", err)
		}
	}
	if cmdErr != nil {
		return cmdErr
	}
	// Check writeConcernError.
	wcRes := writeResult{}
	err := bson.Unmarshal(raw, &wcRes)
	if err != nil {
		return errors.Errorf("failed to unmarshal bson.Raw into writeResult: %v", err)
	}

	if wcRes.ConcernError != nil {
		return wcRes.ConcernError
	}

	return nil
}

// TODO: Look into Command Runner
func createIndexes(database CommandRunner, collection string, indexes []bson.D) error {
	// We create all indexes belonging to a single collection in one command
	// so the server can build the indexes in a single collection scan.
	createIndexesCmd := bson.D{
		{"createIndexes", collection},
		{"indexes", indexes}}
	return runWithWriteConcernMajority(database, createIndexesCmd, nil)
}

// WaitForWriteConcernMajority runs a no-op applyOps with writeConcern majority
// to simulate a writeConcern on the last operation.
func WaitForWriteConcernMajority(s *mongo.Client) error {
	rawOp, err := bson.Marshal(noopOplog)
	if err != nil {
		return err
	}
	_, err = ApplyOpsBatch(&DbCommandRunner{
		Database: s.Database("admin"),
	}, []bson.Raw{rawOp})
	return err
}

// RemoveKey removes the given key. Returns the removed value and true if the
// key was found.
func RemoveKey(key string, document *bson.D) (interface{}, bool) {
	if document == nil {
		return nil, false
	}
	doc := *document
	for i, elem := range doc {
		if elem.Key == key {
			// Remove this key.
			*document = append(doc[:i], doc[i+1:]...)
			return elem.Value, true
		}
	}
	return nil, false
}

// applyOpsBatch applies a batch of oplog operations using applyOps.
func applyOpsBatchBypassValidation(toSession CommandRunner, entries []bson.Raw, bypassValidation bool) (*ApplyOpsResponse, error) {
	if len(entries) == 0 {
		return nil, ErrEmptyApplyOps
	}

	var dummyCommand = Oplog{
		Namespace: "noop.$cmd",
		Operation: "c",
		Object:    bson.D{{"applyOps", []Oplog{noopOplog}}},
	}

	if len(entries) > 1 {
		// MGOMIRROR-61: send a dummy command with each batch to force it to be
		// non-atomic.
		rawDummyCommand, err := bson.Marshal(dummyCommand)
		if err != nil {
			return nil, err
		}
		entries = append(entries, rawDummyCommand)
	}
	res := &ApplyOpsResponse{}
	cmd := bson.D{{"applyOps", entries}}
	if bypassValidation {
		cmd = append(cmd, bson.E{"bypassDocumentValidation", true})
	}
	err := runWithWriteConcernMajority(toSession, cmd, res)
	// The ApplyOpsResponse will contain more useful information than the mgo
	// error when an error response is returned by the server. For example,
	// the "results" array is used to figure out exactly which operation failed.
	// Only return the ApplyOpsResponse if it was initialized.
	if err != nil {
		if res.ErrMsg == "" {
			return nil, err
		}
		return res, err
	}

	// Check the server's response for an error.
	if res.Ok == 0 {
		return res, errors.Errorf("error applying operations: %#v", res)
	}

	return res, nil
}

// withWMajority returns the given command with write concern majority
func withWMajority(cmd bson.D) bson.D {
	return append(cmd, bson.E{"writeConcern", bson.D{{"w", "majority"}}})
}

// The dummy applyOps command cannot be empty.
var noopOplog = Oplog{Operation: "n", Namespace: "", Object: bson.D{{"msg", "mongomirror noop"}}}

// ApplyOpsBatch applies a batch of oplog operations using applyOps.
func ApplyOpsBatch(toSession CommandRunner, entries []bson.Raw) (*ApplyOpsResponse, error) {
	return applyOpsBatchBypassValidation(toSession, entries, true)
}

var ErrEmptyApplyOps = errors.New("cannot send an empty applyOps!")
