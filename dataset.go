// dataset is a modified format of the frictionless data datapackge format http://specs.frictionlessdata.io/data-packages
package dataset

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
)

type Dataset struct {
	// not required, but if it's here, it's gotta match the base of path
	Name Name `json:"name,omitempty"`
	// required for use with other datasets. a dataset's name is the base of this path
	Path Path `json:"path,omitempty"`

	// at most one of these can be set
	Url  string `json:"url,omitempty"`
	File string `json:"file,omitempty"`
	Data []byte `json:"data,omitempty"`
	// This guy is required if data is going to be set
	Format DataFormat `json:"format,omitempty"`
	// This stuff defines the 'schema' for a dataset's data
	Fields     []*Field `json:"fields,omitempty"`
	PrimaryKey FieldKey `json:"primaryKey,omitempty"`
	// optional-but-sometimes-necessary info
	Mediatype string `json:"mediatype,omitempty"`
	Encoding  string `json:"encoding,omitempty"`
	Bytes     int    `json:"bytes,omitempty"`
	Hash      string `json:"hash,omitempty"`

	// A dataset can have child datasets
	Datasets []*Dataset `json:"datasets,omitempty"`
	// optional stufffff
	Author       *Person   `json:"author,omitempty"`
	Title        string    `json:"title,omitempty"`
	Image        string    `json:"image,omitempty"`
	Description  string    `json:"description,omitempty"`
	Homepage     string    `json:"homepage,omitempty"`
	License      *License  `json:"license,omitempty"`
	Version      Version   `json:"version,omitempty"`
	Keywords     []string  `json:"keywords,omitempty"`
	Contributors []*Person `json:"contributors,omitempty"`
	Sources      []*Source `json:"sources,omitempty"`
}

// FetchBytes grabs the actual byte data that this resource represents
// path is the path to the datapackage, and only needed if using the "path"
// resource param
func (r *Dataset) FetchBytes(path string) ([]byte, error) {
	if len(r.Data) > 0 {
		return r.Data, nil
	} else if r.File != "" {
		return ioutil.ReadFile(filepath.Join(path, r.File))
	} else if r.Url != "" {
		res, err := http.Get(r.Url)
		if err != nil {
			return nil, err
		}

		defer res.Body.Close()
		return ioutil.ReadAll(res.Body)
	}

	return nil, fmt.Errorf("resource %s doesn't contain a url, file, or data field to read from", r.Name)
}

func (r *Dataset) Reader() (io.Reader, error) {
	if len(r.Data) > 0 {
		return ioutil.NopCloser(bytes.NewBuffer(r.Data)), nil
	} else if r.File != "" {
		return os.Open(r.File)
	} else if r.Url != "" {
		res, err := http.Get(r.Url)
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	}
	return nil, fmt.Errorf("resource %s doesn't contain a url, file, or data field to read from", r.Name)
}

type dataWriter struct {
	buffer  *bytes.Buffer
	onClose func([]byte)
}

func (w dataWriter) Write(p []byte) (n int, err error) {
	return w.Write(p)
}

func (w dataWriter) Close() error {
	data, err := json.Marshal(w.buffer.Bytes())
	if err != nil {
		w.onClose(data)
	}
	return err
}

func (r *Dataset) Writer() (io.WriteCloser, error) {
	if len(r.Data) > 0 {
		return dataWriter{buffer: bytes.NewBuffer(r.Data), onClose: func(data []byte) { r.Data = data }}, nil
	} else if r.File != "" {
		return os.Open(r.File)
	} else if r.Url != "" {
		return nil, fmt.Errorf("can't write to url-based resource: %s", r.Url)
	}

	return nil, fmt.Errorf("resource %s doesn't contain a path or data field to write to", r.Name)
}

// truthCount returns the number of arguments that are true
func truthCount(args ...bool) (count int) {
	for _, arg := range args {
		if arg {
			count++
		}
	}
	return
}

// separate type for marshalling into
type _dataset Dataset

// UnmarhalJSON can marshal in two forms: just an id string, or an object containing a full data model
func (d *Dataset) UnmarshalJSON(data []byte) error {
	ds := _dataset{}
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	*d = Dataset(ds)
	if err := d.ValidDataSource(); err != nil {
		return err
	}

	return nil
}

func (ds *Dataset) ValidDataSource() error {
	if count := truthCount(ds.Url != "", ds.File != "", len(ds.Data) > 0); count > 1 {
		return errors.New("only one of url, file, or data can be set")
	} else if count == 1 {
		if ds.Format == UnknownDataFormat {
			return errors.New("format is required for data source")
		}
	}

	return nil
}

func (ds *Dataset) RowToStrings(row []interface{}) (strs []string, err error) {
	if len(row) != len(ds.Fields) {
		err = fmt.Errorf("row is not the same length as the dataset's fields")
		return
	}
	strs = make([]string, len(ds.Fields))
	for i, field := range ds.Fields {
		str, err := field.Type.ValueToString(row[i])
		if err != nil {
			return nil, err
		}
		strs[i] = str
	}
	return
}

func (ds *Dataset) RowToBytes(row []interface{}) (bytes [][]byte, err error) {
	if len(row) != len(ds.Fields) {
		err = fmt.Errorf("row is not the same length as the dataset's fields")
		return
	}
	bytes = make([][]byte, len(ds.Fields))
	for i, field := range ds.Fields {
		val, err := field.Type.ValueToBytes(row[i])
		if err != nil {
			return nil, err
		}
		bytes[i] = val
	}
	return
}