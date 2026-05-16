package xmeta

import (
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
)

func TestStringAnyHelpers(t *testing.T) {
	packed := PackStringAny("CURRENT_TIMESTAMP")
	if packed == nil {
		t.Fatal("expected packed string Any")
	}
	if got := UnpackStringAny(packed); got != "CURRENT_TIMESTAMP" {
		t.Fatalf("unpacked string = %q", got)
	}
	if got := PackStringAny(""); got != nil {
		t.Fatalf("empty string packed as %#v, want nil", got)
	}
	if got := UnpackStringAny(nil); got != "" {
		t.Fatalf("nil Any unpacked as %q", got)
	}
	if got := UnpackStringAny(&anypb.Any{TypeUrl: "type.googleapis.com/sqlmeta.Unknown", Value: []byte{1, 2, 3}}); got != "" {
		t.Fatalf("unknown Any unpacked as %q", got)
	}
}
