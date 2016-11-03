package dataset

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func TestDatasetUnmarshallJSON(t *testing.T) {
	cases := []struct {
		FileName string
		Name     Name
		err      error
	}{
		{"airport-codes.json", "airport-codes", nil},
		{"continent-codes.json", "continent-codes", nil},
	}

	for i, c := range cases {
		data, err := ioutil.ReadFile(filepath.Join("test_package_files", c.FileName))
		if err != nil {
			t.Errorf("case %d couldn't read file: %s", i, err.Error())
		}

		ds := &Dataset{}
		if err := json.Unmarshal(data, ds); err != c.err {
			t.Errorf("case %d parse error mismatch. expected: '%s', got: '%s'", i, c.err, err)
		}

		if ds.Name != c.Name {
			t.Errorf("case %d dataset name mismatch. expected: '%s', got: '%s'", i, c.Name, ds.Name)
		}

	}
}