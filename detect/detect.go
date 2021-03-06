package detect

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
)

var (
	spaces   = regexp.MustCompile(`[\s-]+`)
	nonAlpha = regexp.MustCompile(`[^a-zA-z0-9_]`)
	log      = logger.Logger("detect")
)

// FromFile takes a filepath & tries to work out the corresponding dataset
// for the sake of speed, it only works with files that have a recognized extension
func FromFile(path string) (ds *dataset.Structure, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return FromReader(path, f)
}

// FromReader is a shorthand for a path/filename and reader
func FromReader(path string, data io.Reader) (ds *dataset.Structure, err error) {
	format, err := ExtensionDataFormat(path)
	if err != nil {
		return nil, err
	}
	return Structure(format, data)
}

// Structure attemptes to extract a structure based on a given format and data reader
func Structure(format dataset.DataFormat, data io.Reader) (r *dataset.Structure, err error) {
	r = &dataset.Structure{
		Format: format,
	}
	// ds.Data = ReplaceSoloCarriageReturns(ds.Data)
	r.Schema, err = Schema(r, data)
	return
}

// DataFormat does it's best to determine the format of a specified dataset
// func DataFormat(path string) (format dataset.DataFormat, err error) {
// 	return ExtensionDataFormat(path)
// }

// ExtensionDataFormat returns the corresponding DataFormat for a given file extension if one exists
// TODO - this should probably come from the dataset package
func ExtensionDataFormat(path string) (format dataset.DataFormat, err error) {
	ext := filepath.Ext(path)
	switch ext {
	case ".cbor":
		return dataset.CBORDataFormat, nil
	case ".json":
		return dataset.JSONDataFormat, nil
	case ".csv":
		return dataset.CSVDataFormat, nil
	case ".xml":
		return dataset.XMLDataFormat, nil
	case ".xls":
		return dataset.XLSDataFormat, nil
	case "":
		return dataset.UnknownDataFormat, errors.New("no file extension provided")
	default:
		return dataset.UnknownDataFormat, fmt.Errorf("unsupported file type: '%s'", ext)
	}
}
