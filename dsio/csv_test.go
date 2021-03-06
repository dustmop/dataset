package dsio

import (
	"bytes"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

const csvData = `col_a,col_b,col_c,col_d,col_3,col_f,col_g
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null
a,1.23,4,false,"{""a"":""b""}","[1,2,3]",null`

var csvStruct = &dataset.Structure{
	Format: dataset.CSVDataFormat,
	FormatConfig: &dataset.CSVOptions{
		HeaderRow: true,
	},
	Schema: jsonschema.Must(`{
		"type": "array",
		"items": {
			"type":"array",
			"items": [
				{"title":"col_a","type":"string"},
				{"title":"col_b","type":"number"},
				{"title":"col_c","type":"integer"},
				{"title":"col_d","type":"boolean"},
				{"title":"col_e","type":"object"},
				{"title":"col_f","type":"array"},
				{"title":"col_g","type":"null"}
			]
		}
	}`),
}

func TestCSVReader(t *testing.T) {
	buf := bytes.NewBuffer([]byte(csvData))
	rdr, err := NewEntryReader(csvStruct, buf)
	if err != nil {
		t.Errorf("error allocating EntryReader: %s", err.Error())
		return
	}
	count := 0
	for {
		ent, err := rdr.ReadEntry()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Errorf("unexpected error: %s", err.Error())
			return
		}

		if arr, ok := ent.Value.([]interface{}); ok {
			if len(arr) != 7 {
				t.Errorf("invalid row length for row %d. expected %d, got %d", count, 7, len(arr))
				continue
			}
		} else {
			t.Errorf("expected value to []interface{}. got: %#v", ent.Value)
			continue
		}

		count++
	}
	if count != 5 {
		t.Errorf("expected: %d rows, got: %d", 5, count)
	}
}

func TestCSVWriter(t *testing.T) {
	rows := []Entry{
		// TODO - vary up test input
		{Value: []interface{}{"a", float64(12), 23, nil}},
		{Value: []interface{}{"a", float64(12), 23, []interface{}{"foo", "bar"}}},
		{Value: []interface{}{"a", float64(12), 23, map[string]interface{}{"foo": "bar"}}},
		{Value: []interface{}{"a", float64(12), int64(23), false}},
		{Value: []interface{}{"a", float64(12), 23, false}},
	}

	buf := &bytes.Buffer{}
	rw, err := NewEntryWriter(csvStruct, buf)
	if err != nil {
		t.Errorf("error allocating EntryWriter: %s", err.Error())
		return
	}
	st := rw.Structure()
	if err := dataset.CompareStructures(st, csvStruct); err != nil {
		t.Errorf("structure mismatch: %s", err.Error())
		return
	}

	for i, row := range rows {
		if err := rw.WriteEntry(row); err != nil {
			t.Errorf("row %d write error: %s", i, err.Error())
		}
	}

	if err := rw.Close(); err != nil {
		t.Errorf("close reader error: %s", err.Error())
		return
	}
	if bytes.Equal(buf.Bytes(), []byte(csvData)) {
		t.Errorf("output mismatch. %s != %s", buf.String(), csvData)
	}
}

func TestReplaceSoloCarriageReturns(t *testing.T) {
	input := []byte("foo\r\rbar\r\nbaz\r\r")
	expect := []byte("foo\r\n\r\nbar\r\nbaz\r\n\r\n")

	got := make([]byte, 19)
	n, err := ReplaceSoloCarriageReturns(bytes.NewReader(input)).Read(got)
	if err != nil && err.Error() != "EOF" {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if n != 19 {
		t.Errorf("length error. expected: %d, got: %d", 19, n)
	}
	if !bytes.Equal(expect, got) {
		t.Errorf("byte mismatch. expected:\n%v\ngot:\n%v", expect, got)
	}
}
