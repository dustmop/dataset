package dsio

import (
	"bytes"
	"github.com/qri-io/dataset/vals"
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/jsonschema"
)

const csvData = `col_a,col_b,col_c,col_d
a,b,c,d
a,b,c,d
a,b,c,d
a,b,c,d
a,b,c,d`

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
				{"title":"col_b","type":"string"},
				{"title":"col_c","type":"string"},
				{"title":"col_d","type":"string"}
			]
		}
	}`),
}

func TestCSVReader(t *testing.T) {
	buf := bytes.NewBuffer([]byte(csvData))
	rdr, err := NewValueReader(csvStruct, buf)
	if err != nil {
		t.Errorf("error allocating ValueReader: %s", err.Error())
		return
	}
	count := 0
	for {
		row, err := rdr.ReadValue()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Errorf("unexpected error: %s", err.Error())
			return
		}

		if row.Type() != vals.TypeArray {
			t.Errorf("expected value to be an Array. got: %s", row.Type())
			continue
		}

		if row.Len() != 4 {
			t.Errorf("invalid row length for row %d. expected %d, got %d", count, 4, row.Len())
		}

		count++
	}
	if count != 5 {
		t.Errorf("expected: %d rows, got: %d", 5, count)
	}
}

func TestCSVWriter(t *testing.T) {
	rows := []vals.Array{
		// TODO - vary up test input
		vals.Array{vals.String("a"), vals.String("b"), vals.String("c"), vals.String("d")},
		vals.Array{vals.String("a"), vals.String("b"), vals.String("c"), vals.String("d")},
		vals.Array{vals.String("a"), vals.String("b"), vals.String("c"), vals.String("d")},
		vals.Array{vals.String("a"), vals.String("b"), vals.String("c"), vals.String("d")},
		vals.Array{vals.String("a"), vals.String("b"), vals.String("c"), vals.String("d")},
	}

	buf := &bytes.Buffer{}
	rw, err := NewValueWriter(csvStruct, buf)
	if err != nil {
		t.Errorf("error allocating ValueWriter: %s", err.Error())
		return
	}
	st := rw.Structure()
	if err := dataset.CompareStructures(st, csvStruct); err != nil {
		t.Errorf("structure mismatch: %s", err.Error())
		return
	}

	for i, row := range rows {
		if err := rw.WriteValue(row); err != nil {
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
