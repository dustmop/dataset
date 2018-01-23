package validate

// import (
// 	"fmt"
// 	"io"
// 	"strconv"

// 	"github.com/datatogether/cdxj"
// 	"github.com/qri-io/dataset"
// 	"github.com/qri-io/dataset/dsio"
// 	"github.com/qri-io/dataset/vals"
// )

// // DataFormat ensures that for each accepted dataset.DataFormat,
// // we havea well-formed dataset (eg. for csv, we need rows to all
// // be of same length)
// func DataFormat(df dataset.DataFormat, r io.Reader) error {
// 	switch df {
// 	// explicitly supported at present
// 	case dataset.CSVDataFormat:
// 		return CheckCsvRowLengths(r)
// 	case dataset.CDXJDataFormat:
// 		return cdxj.Validate(r)
// 	// explicitly unsupported at present
// 	case dataset.JSONDataFormat:
// 		return fmt.Errorf("error: data format 'JsonData' not currently supported")
// 	case dataset.XLSDataFormat:
// 		return fmt.Errorf("error: data format 'XlsData' not currently supported")
// 	case dataset.XMLDataFormat:
// 		return fmt.Errorf("error: data format 'XmlData' not currently supported")
// 	// *implicitly unsupported
// 	case dataset.UnknownDataFormat:
// 		return fmt.Errorf("error: unknown data format not currently supported")
// 	default:
// 		return fmt.Errorf("error: data format not currently supported")
// 	}
// }

// const (
// 	// ErrFmtOneHotMatrix designates a dataset of errors, with errors designated by
// 	// 1's, no error designated by 0's
// 	ErrFmtOneHotMatrix = "oneHotMatrix"
// 	// ErrFmtString designates a dataset with empty string signifying no error,
// 	// and a string representation of the error when an error is present
// 	ErrFmtString = "string"
// )

// // DataErrorsCfg configures the DataErrors function
// type DataErrorsCfg struct {
// 	ErrorFormat string
// 	// DataFormat  DataFormat
// }

// // DefaultDataErrorsCfg is the default configuration for
// // the DataErrors function
// func DefaultDataErrorsCfg() *DataErrorsCfg {
// 	return &DataErrorsCfg{
// 		ErrorFormat: ErrFmtString,
// 	}
// }

// // DataErrors generates a new dataset that represents data errors with the passed in dataset reader
// func DataErrors(r dsio.ValueReader, options ...func(*DataErrorsCfg)) (errors dsio.ValueReader, count int, err error) {
// 	cfg := DefaultDataErrorsCfg()
// 	for _, opt := range options {
// 		opt(cfg)
// 	}

// 	vst := &dataset.Structure{
// 		Format: dataset.CSVDataFormat,
// 		Schema: &dataset.Schema{
// 			Fields: []*dataset.Field{
// 				{Name: "row_index", Type: vals.Integer},
// 			},
// 		},
// 	}
// 	for _, f := range r.Structure().Schema.Fields {
// 		vst.Schema.Fields = append(vst.Schema.Fields, &dataset.Field{Name: f.Name + "_error", Type: vals.String})
// 	}

// 	buf, err := dsio.NewStructuredBuffer(vst)
// 	if err != nil {
// 		return nil, 0, fmt.Errorf("error creating a row buffer: %s", err.Error())
// 	}

// 	err = dsio.EachRow(r, func(num int, row [][]byte, err error) error {
// 		if err != nil {
// 			return err
// 		}

// 		errData, errNum, err := validateRow(r.Structure().Schema.Fields, num, row)
// 		if err != nil {
// 			return err
// 		}

// 		count += errNum
// 		if errNum != 0 {
// 			buf.WriteRow(errData)
// 		}

// 		return nil
// 	})
// 	if err != nil {
// 		return
// 	}

// 	if err = buf.Close(); err != nil {
// 		err = fmt.Errorf("error closing valdation buffer: %s", err.Error())
// 		return
// 	}

// 	errors = buf
// 	return
// }

// func validateRow(fields []*dataset.Field, num int, row [][]byte) ([][]byte, int, error) {
// 	count := 0
// 	errors := make([][]byte, len(fields)+1)
// 	errors[0] = []byte(strconv.FormatInt(int64(num), 10))
// 	if len(row) != len(fields) {
// 		return errors, count, fmt.Errorf("column mismatch. expected: %d, got: %d", len(fields), len(row))
// 	}

// 	for i, f := range fields {
// 		_, e := f.Type.Parse(row[i])
// 		if e != nil {
// 			count++
// 			errors[i+1] = []byte(e.Error())
// 		} else {
// 			errors[i+1] = []byte("")
// 		}
// 	}

// 	return errors, count, nil
// }
