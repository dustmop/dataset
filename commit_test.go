package dataset

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
)

func TestCommit(t *testing.T) {
	ref := NewCommitRef(datastore.NewKey("a"))
	if !ref.IsEmpty() {
		t.Errorf("expected reference to be empty")
	}

	if !ref.Path().Equal(datastore.NewKey("a")) {
		t.Errorf("expected ref path to equal /a")
	}
}

func TestCommitAssign(t *testing.T) {
	t1 := time.Now()
	doug := &User{ID: "doug_id", Email: "doug@example.com"}
	expect := &Commit{
		path:      datastore.NewKey("a"),
		Kind:      KindCommit,
		Author:    doug,
		Timestamp: t1,
		Title:     "expect title",
		Message:   "expect message",
	}
	got := &Commit{
		Author:  &User{ID: "maha_id", Email: "maha@example.com"},
		Title:   "title",
		Message: "message",
	}

	got.Assign(&Commit{
		Author: doug,
		Kind:   KindCommit,
		Title:  "expect title",
	}, &Commit{
		path:      datastore.NewKey("a"),
		Timestamp: t1,
		Message:   "expect message",
	})

	if err := CompareCommits(expect, got); err != nil {
		t.Error(err)
	}

	got.Assign(nil, nil)
	if err := CompareCommits(expect, got); err != nil {
		t.Error(err)
	}

	emptyMsg := &Commit{}
	emptyMsg.Assign(expect)
	if err := CompareCommits(expect, emptyMsg); err != nil {
		t.Error(err)
	}
}

func TestCommitMarshalJSON(t *testing.T) {
	ts := time.Date(2001, 01, 01, 01, 01, 01, 0, time.UTC)
	cases := []struct {
		in  *Commit
		out []byte
		err error
	}{
		{&Commit{Title: "title", Timestamp: ts}, []byte(`{"kind":"qri:cm:0","timestamp":"2001-01-01T01:01:01Z","title":"title"}`), nil},
		{&Commit{Author: &User{ID: "foo"}, Timestamp: ts}, []byte(`{"author":{"id":"foo"},"kind":"qri:cm:0","timestamp":"2001-01-01T01:01:01Z","title":""}`), nil},
	}

	for i, c := range cases {
		got, err := c.in.MarshalJSON()
		if err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if !bytes.Equal(c.out, got) {
			t.Errorf("case %d error mismatch. %s != %s", i, string(c.out), string(got))
			continue
		}
	}

	strbytes, err := json.Marshal(&Commit{path: datastore.NewKey("/path/to/dataset")})
	if err != nil {
		t.Errorf("unexpected string marshal error: %s", err.Error())
		return
	}

	if !bytes.Equal(strbytes, []byte("\"/path/to/dataset\"")) {
		t.Errorf("marshal strbyte interface byte mismatch: %s != %s", string(strbytes), "\"/path/to/dataset\"")
	}
}

func TestCommitUnmarshalJSON(t *testing.T) {
	cases := []struct {
		data   string
		result *Commit
		err    error
	}{
		{`{}`, &Commit{}, nil},
		{`{ "title": "title", "message": "message"}`, &Commit{Title: "title", Message: "message"}, nil},
		{`{ "author" : { "id": "id", "email": "email@email.com"} }`, &Commit{Author: &User{ID: "id", Email: "email@email.com"}}, nil},
	}

	for i, c := range cases {
		cm := &Commit{}
		if err := json.Unmarshal([]byte(c.data), cm); err != c.err {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}

		if err := CompareCommits(cm, c.result); err != nil {
			t.Errorf("case %d comparison error: %s", i, err)
			continue
		}
	}

	strq := &Commit{}
	path := "/path/to/msg"
	if err := json.Unmarshal([]byte(`"`+path+`"`), strq); err != nil {
		t.Errorf("unmarshal string path error: %s", err.Error())
		return
	}

	if strq.path.String() != path {
		t.Errorf("unmarshal didn't set proper path: %s != %s", path, strq.path)
		return
	}
}

func TestUnmarshalCommit(t *testing.T) {
	cma := Commit{Kind: KindCommit, Message: "foo"}
	cases := []struct {
		value interface{}
		out   *Commit
		err   string
	}{
		{cma, &cma, ""},
		{&cma, &cma, ""},
		{[]byte("{\"kind\":\"qri:cm:0\"}"), &Commit{Kind: KindCommit}, ""},
		{5, nil, "couldn't parse commitMsg, value is invalid type"},
	}

	for i, c := range cases {
		got, err := UnmarshalCommit(c.value)
		if !(err == nil && c.err == "" || err != nil && err.Error() == c.err) {
			t.Errorf("case %d error mismatch. expected: '%s', got: '%s'", i, c.err, err)
			continue
		}
		if err := CompareCommits(c.out, got); err != nil {
			t.Errorf("case %d dataset mismatch: %s", i, err.Error())
			continue
		}
	}
}
