package bsonutil

import (
	"github.com/mongodb/mongo-tools-common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ConvertLegacyIndexKeys transforms the values of index definitions pre 3.4 into
// the stricter index definitions of 3.4+. Prior to 3.4, any value in an index key
// that isn't a negative number or that isn't a string is treated as 1.
// The one exception is an empty string is treated as 1.
// All other strings that aren't one of ["2d", "geoHaystack", "2dsphere", "hashed", "text", ""]
// will cause the index build to fail. See TOOLS-2412 for more information.
//
// Note, this function doesn't convert Decimal values which are equivalent to "0" (e.g. 0.00 or -0).
//
// This function logs the keys that are converted.
func ConvertLegacyIndexKeys(indexKey bson.D, ns string) {
	var converted bool
	originalJSONString := CreateExtJSONString(indexKey)
	for j, elem := range indexKey {
		switch v := elem.Value.(type) {
		case int32, int64, float64:
			// Only convert 0 value
			if v == 0 {
				indexKey[j].Value = 1
				converted = true
			}
		case primitive.Decimal128:
			// Note, this doesn't catch Decimal values which are equivalent to "0" (e.g. 0.00 or -0).
			// These values are so unlikely we just ignore them
			zeroVal, err := primitive.ParseDecimal128("0")
			if err == nil {
				if v == zeroVal {
					indexKey[j].Value = 1
					converted = true
				}
			}
		case string:
			// Only convert an empty string
			if v == "" {
				indexKey[j].Value = 1
				converted = true
			}
		default:
			// Convert all types that aren't strings or numbers
			indexKey[j].Value = 1
			converted = true
		}
	}
	if converted {
		newJSONString := CreateExtJSONString(indexKey)
		log.Logvf(log.Always, "convertLegacyIndexes: converted index values '%s' to '%s' on collection '%s'",
			originalJSONString, newJSONString, ns)
	}
}

// CreateExtJSONString stringifies doc as Extended JSON. It does not error
// if it's unable to marshal the doc to JSON.
func CreateExtJSONString(doc interface{}) string {
	// by default return "<unable to format document>"" since we don't
	// want to throw an error when formatting informational messages.
	// An error would be inconsequential.
	JSONString := "<unable to format document>"
	JSONBytes, err := bson.MarshalExtJSON(doc, false, false)
	if err == nil {
		JSONString = string(JSONBytes)
	}
	return JSONString
}
