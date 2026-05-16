package xmeta

import (
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// PackStringAny packs a non-empty string as a google.protobuf.StringValue Any.
func PackStringAny(s string) *anypb.Any {
	if s == "" {
		return nil
	}
	anyVal, err := anypb.New(&wrapperspb.StringValue{Value: s})
	if err != nil {
		return nil
	}
	return anyVal
}

// UnpackStringAny returns the string value from a StringValue Any.
// Unknown Any payloads and nil values intentionally render as empty strings.
func UnpackStringAny(a *anypb.Any) string {
	if a == nil {
		return ""
	}
	var sv wrapperspb.StringValue
	if err := a.UnmarshalTo(&sv); err != nil {
		return ""
	}
	return sv.Value
}
