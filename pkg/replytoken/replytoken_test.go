package replytoken

import (
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestNewIsSixtyFourHexCharactersAndNeverRepeats(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 2000; i++ {
		tok, err := New()
		if err != nil {
			t.Fatal(err)
		}
		if len(tok) != 2*Size || strings.Trim(tok, "0123456789abcdef") != "" {
			t.Fatalf("token %q is not %d lower-case hex characters", tok, 2*Size)
		}
		if seen[tok] {
			t.Fatal("a token repeated")
		}
		seen[tok] = true
	}
}

func TestMatchesIsExact(t *testing.T) {
	tok, _ := New()
	with := func(v ...string) *nats.Msg { return &nats.Msg{Header: nats.Header{Header: v}} }
	if !Matches(with(tok), tok) {
		t.Fatal("the token did not match itself")
	}
	for name, msg := range map[string]*nats.Msg{
		"nil message":             nil,
		"no header":               {},
		"empty value":             with(""),
		"a prefix":                with(tok[:len(tok)-1]),
		"an extension":            with(tok + "0"),
		"another token":           with(strings.Repeat("0", 2*Size)),
		"upper case":              with(strings.ToUpper(tok)),
		"padded":                  with(" " + tok),
		"another header's value":  {Header: nats.Header{"Other": []string{tok}}},
		"token second, not first": with("x", tok),
	} {
		if Matches(msg, tok) {
			t.Fatalf("%s matched", name)
		}
	}
	// An empty expected token matches nothing, including a message with none.
	if Matches(&nats.Msg{}, "") || Matches(with(""), "") {
		t.Fatal("an empty token matched")
	}
}

func TestEchoCopiesOnlyTheToken(t *testing.T) {
	if Echo(&nats.Msg{}) != nil || Echo(nil) != nil || EchoToken("") != nil {
		t.Fatal("a request with no token produced an echo header")
	}
	tok, _ := New()
	h := Echo(&nats.Msg{Header: nats.Header{Header: []string{tok}, "Authorization": []string{"secret"}, "Other": []string{"x"}}})
	if len(h) != 1 || h.Get(Header) != tok {
		t.Fatalf("echo = %v, want only the token", h)
	}
}
