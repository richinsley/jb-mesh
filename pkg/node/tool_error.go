package node

import (
	"errors"
	"strings"
)

// tracebackMarker is how a Python traceback begins, wherever it was attached:
// by the service SDK, by the process bridge, or by the interpreter itself.
const tracebackMarker = "Traceback (most recent call last)"

// splitToolError separates what a caller is told when a tool call fails from
// what stays on the node. A raising Python method produces an error whose text
// is the exception's type and message followed by a traceback, and a
// traceback names files on this machine: the tool's directory, its virtual
// environment, the SDK's install path. The caller gets the type and message.
// The traceback is for whoever operates the node: the tool's own process
// writes it to its stderr, and the node logs it with the call's correlation
// id through its configured logger (so a node set to quiet logs nothing).
//
// public is the text up to the first traceback, trimmed; if that leaves
// nothing, it is a fixed sentence, so a caller is never sent an empty error.
// traceback is the rest, or "" when the error carried none.
func splitToolError(msg string) (public, traceback string) {
	i := strings.Index(msg, tracebackMarker)
	if i < 0 {
		return strings.TrimSpace(msg), ""
	}
	public = strings.TrimSpace(msg[:i])
	if public == "" {
		public = "the tool raised an error"
	}
	return public, strings.TrimSpace(msg[i:])
}

// publicToolError is the error a caller is sent for err: the same error with
// any traceback removed. It also returns the traceback for the node's own log.
func publicToolError(err error) (error, string) {
	if err == nil {
		return nil, ""
	}
	public, traceback := splitToolError(err.Error())
	if traceback == "" {
		return err, ""
	}
	return errors.New(public), traceback
}

// scrubStreamFrame removes traceback text from one stream frame a tool
// produced, in place, and returns what it removed. A streaming method that
// fails mid-stream reports it in a frame, and that frame goes straight to the
// caller.
func scrubStreamFrame(frame map[string]interface{}) (traceback string) {
	var removed []string
	if tb, ok := frame["traceback"].(string); ok && tb != "" {
		removed = append(removed, tb)
	}
	delete(frame, "traceback")
	switch e := frame["error"].(type) {
	case string:
		public, tb := splitToolError(e)
		if tb != "" {
			frame["error"] = public
			removed = append(removed, tb)
		}
	case map[string]interface{}:
		if tb, ok := e["traceback"].(string); ok && tb != "" {
			removed = append(removed, tb)
		}
		delete(e, "traceback")
		if m, ok := e["message"].(string); ok {
			public, tb := splitToolError(m)
			if tb != "" {
				e["message"] = public
				removed = append(removed, tb)
			}
		}
	}
	return strings.Join(removed, "\n")
}

// splitPublic is the public half of err's text.
func splitPublic(err error) string {
	public, _ := splitToolError(err.Error())
	return public
}
