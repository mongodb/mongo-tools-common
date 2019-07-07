package util

import "testing"

func TestSanitizeURI(t *testing.T) {
	cases := [][]string{
		[]string{"mongodb://example.com/", "mongodb://example.com/"},
		[]string{"mongodb://example.com/?appName=foo:@bar", "mongodb://example.com/?appName=foo:@bar"},
		[]string{"mongodb://example.com?appName=foo:@bar", "mongodb://example.com?appName=foo:@bar"},
		[]string{"mongodb://@example.com/", "mongodb://[**REDACTED**]@example.com/"},
		[]string{"mongodb://:@example.com/", "mongodb://[**REDACTED**]@example.com/"},
		[]string{"mongodb://user@example.com/", "mongodb://[**REDACTED**]@example.com/"},
		[]string{"mongodb://user:@example.com/", "mongodb://[**REDACTED**]@example.com/"},
		[]string{"mongodb://:pass@example.com/", "mongodb://[**REDACTED**]@example.com/"},
		[]string{"mongodb://user:pass@example.com/", "mongodb://[**REDACTED**]@example.com/"},
		[]string{"mongodb+srv://example.com/", "mongodb+srv://example.com/"},
		[]string{"mongodb+srv://@example.com/", "mongodb+srv://[**REDACTED**]@example.com/"},
		[]string{"mongodb+srv://:@example.com/", "mongodb+srv://[**REDACTED**]@example.com/"},
		[]string{"mongodb+srv://user@example.com/", "mongodb+srv://[**REDACTED**]@example.com/"},
		[]string{"mongodb+srv://user:@example.com/", "mongodb+srv://[**REDACTED**]@example.com/"},
		[]string{"mongodb+srv://:pass@example.com/", "mongodb+srv://[**REDACTED**]@example.com/"},
		[]string{"mongodb+srv://user:pass@example.com/", "mongodb+srv://[**REDACTED**]@example.com/"},
	}

	for _, c := range cases {
		got := SanitizeURI(c[0])
		if got != c[1] {
			t.Errorf("For %s: got: %s; wanted: %s", c[0], got, c[1])
		}
	}
}
