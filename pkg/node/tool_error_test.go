package node

import (
	"errors"
	"strings"
	"testing"
)

// The three shapes a raising Python method's error takes on its way to the
// node: the msgpack bridge's, the service SDK's re-raise, and the REPL path's
// type/message plus an appended traceback.
var raisedErrors = map[string]struct{ in, public string }{
	"msgpack bridge": {
		"queue call failed: python error: ValueError: auth_bad_signature: the signature does not match\nTraceback (most recent call last):\n  File \"/home/op/.venvs/tool/lib/python3.11/site-packages/jb_service/msgpack_protocol.py\", line 252, in wrapper\n    result = method(**converted)\n  File \"/home/op/tools/tool/main.py\", line 143, in tally_add\n    raise ValueError(\"auth_bad_signature\")\nValueError: auth_bad_signature\n",
		"queue call failed: python error: ValueError: auth_bad_signature: the signature does not match",
	},
	"sdk re-raise": {
		"python error: Exception: KeyError: 'label'\nTraceback (most recent call last):\n  File \"/srv/node/tools/x/main.py\", line 9, in f\nKeyError: 'label'",
		"python error: Exception: KeyError: 'label'",
	},
	"repl path": {
		"RuntimeError: disk is full\nTraceback (most recent call last):\n  File \"C:\\\\tools\\\\x\\\\main.py\", line 4, in g\nRuntimeError: disk is full",
		"RuntimeError: disk is full",
	},
	"traceback only": {
		"Traceback (most recent call last):\n  File \"/srv/x.py\", line 1\nZeroDivisionError: division by zero",
		"the tool raised an error",
	},
}

func TestSplitToolErrorKeepsTypeAndMessageAndDropsTheTraceback(t *testing.T) {
	for name, tc := range raisedErrors {
		t.Run(name, func(t *testing.T) {
			public, traceback := splitToolError(tc.in)
			if public != tc.public {
				t.Fatalf("public = %q, want %q", public, tc.public)
			}
			if !strings.HasPrefix(traceback, tracebackMarker) {
				t.Fatalf("traceback = %q", traceback)
			}
			for _, leak := range []string{"Traceback", "File \"", "/home/", "/srv/", "site-packages", "main.py", "line "} {
				if strings.Contains(public, leak) {
					t.Fatalf("the public error still carries %q: %q", leak, public)
				}
			}
		})
	}
	// An error with no traceback is passed through unchanged.
	for _, plain := range []string{"tool x is not running, start it first", "queue call failed: python process has exited: <nil>", ""} {
		if public, traceback := splitToolError(plain); public != plain || traceback != "" {
			t.Fatalf("plain error %q became %q / %q", plain, public, traceback)
		}
	}
}

func TestPublicToolError(t *testing.T) {
	if err, tb := publicToolError(nil); err != nil || tb != "" {
		t.Fatalf("nil error became %v / %q", err, tb)
	}
	plain := errors.New("method not found: nope")
	if err, tb := publicToolError(plain); err != plain || tb != "" {
		t.Fatal("an error with no traceback was rewritten")
	}
	raised := errors.New(raisedErrors["msgpack bridge"].in)
	err, tb := publicToolError(raised)
	if err == nil || err.Error() != raisedErrors["msgpack bridge"].public || !strings.Contains(tb, "msgpack_protocol.py") {
		t.Fatalf("raised error became %v / %q", err, tb)
	}
}

// A streaming method that fails mid-stream reports it in a frame that goes
// straight to the caller; the frame is scrubbed the same way.
func TestScrubStreamFrame(t *testing.T) {
	t.Run("error string with a traceback", func(t *testing.T) {
		frame := map[string]interface{}{"error": raisedErrors["sdk re-raise"].in, "done": true}
		tb := scrubStreamFrame(frame)
		if frame["error"] != raisedErrors["sdk re-raise"].public || !strings.Contains(tb, "/srv/node/tools") || frame["done"] != true {
			t.Fatalf("frame = %v, removed %q", frame, tb)
		}
	})
	t.Run("structured error with a traceback field", func(t *testing.T) {
		frame := map[string]interface{}{"error": map[string]interface{}{"type": "ValueError", "message": "bad input", "traceback": "Traceback (most recent call last):\n  File \"/srv/x.py\""}}
		tb := scrubStreamFrame(frame)
		e := frame["error"].(map[string]interface{})
		if _, has := e["traceback"]; has || e["type"] != "ValueError" || e["message"] != "bad input" || !strings.Contains(tb, "/srv/x.py") {
			t.Fatalf("frame = %v, removed %q", frame, tb)
		}
	})
	t.Run("top-level traceback field", func(t *testing.T) {
		frame := map[string]interface{}{"error": "boom", "traceback": "Traceback (most recent call last):\n  File \"/srv/y.py\""}
		tb := scrubStreamFrame(frame)
		if _, has := frame["traceback"]; has || frame["error"] != "boom" || !strings.Contains(tb, "/srv/y.py") {
			t.Fatalf("frame = %v, removed %q", frame, tb)
		}
	})
	t.Run("an ordinary frame is untouched", func(t *testing.T) {
		frame := map[string]interface{}{"chunk": "Traceback is just a word here", "done": false}
		if tb := scrubStreamFrame(frame); tb != "" || frame["chunk"] != "Traceback is just a word here" {
			t.Fatalf("an ordinary frame was changed: %v / %q", frame, tb)
		}
	})
}
