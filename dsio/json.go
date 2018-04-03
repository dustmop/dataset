package dsio

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
)

// JSONReader implements the RowReader interface for the JSON data format
type JSONReader struct {
	rowsRead    int
	initialized bool
	scanMode    scanMode // are we scanning an object or an array? default: array.
	st          *dataset.Structure
	d           *json.Decoder
}

// NewJSONReader creates a reader from a structure and read source
func NewJSONReader(st *dataset.Structure, r io.Reader) (*JSONReader, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for JSON reader")
		log.Debug(err.Error())
		return nil, err
	}

	d := json.NewDecoder(r)
	jr := &JSONReader{
		st: st,
		d: d,
	}
	sm, err := schemaScanMode(st.Schema)
	if err != nil {
		return nil, err
	}
	jr.scanMode = sm
	// Begining of object or array, starts with delimiter.
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil, err
	}
	if jr.scanMode == smObject && delim != '{' {
		return nil, fmt.Errorf("Expected: opening { for JSON object")
	} else if jr.scanMode == smArray && delim != '[' {
		return nil, fmt.Errorf("Expected: opening [ for JSON array")
        }
	return jr, err
}

// Structure gives this writer's structure
func (r *JSONReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one JSON record from the reader
func (r *JSONReader) ReadEntry() (Entry, error) {
	if r.scanMode == smObject {
		return r.readObjectEntry()
	} else {
		return r.readArrayEntry()
	}
}

func (r *JSONReader) readObjectEntry() (Entry, error) {
	ent := Entry{}
	// Check if json object is closing, or if token stream abruptly ends.
	tok, err := r.d.Token()
	if err != nil {
		if err.Error() == "EOF" {
			return ent, fmt.Errorf("did not find closing '}'")
		}
		return ent, err
	}
	delim, ok := tok.(json.Delim)
	if ok && delim == '}' {
		// TODO: Make sure there's no more tokens in the decoder.
		return ent, fmt.Errorf("EOF")
	}
	// Convert tokens to key:value pair.
	ent.Key = tok.(string)
	tok, err = r.d.Token()
	if err != nil {
		return ent, err
	}
	ent.Value, err = r.makeValue(tok)
	r.rowsRead++
	return ent, err
}

func (r *JSONReader) readArrayEntry() (Entry, error) {
	ent := Entry{}
	tok, err := r.d.Token()
	if err != nil {
		if err.Error() == "EOF" {
			return ent, fmt.Errorf("did not find closing ']'")
		}
		return ent, err
	}
	delim, ok := tok.(json.Delim)
	if ok && delim == ']' {
		// TODO: Make sure there's no more tokens in the decoder.
		return ent, fmt.Errorf("EOF")
	}
	// Read next entry in array.
	ent.Index = r.rowsRead
	ent.Value, err = r.makeValue(tok)
	r.rowsRead++
	return ent, err
}

func (r *JSONReader) makeValue(tok json.Token) (interface{}, error) {
	switch v := tok.(type) {
	case bool, int, float64, string, nil:
		return v, nil
	case json.Delim:
		if v == '{' {
			inner := make(map[string]interface{})
			for {
				ent, err := r.readObjectEntry()
				if err != nil {
					if err.Error() == "EOF" {
						break
					}
					return nil, err
				}
				inner[ent.Key] = ent.Value
			}
			return inner, nil
		} else if v == '[' {
			inner := make([]interface{}, 0)
			for {
				ent, err := r.readArrayEntry()
				if err != nil {
					if err.Error() == "EOF" {
						break
					}
					return nil, err
				}
				inner = append(inner, ent.Value)
			}
			return inner, nil
		}
	}
	return nil, fmt.Errorf("Unexpected value %v", tok)
}

// JSONWriter implements the RowWriter interface for
// JSON-formatted data
type JSONWriter struct {
	rowsWritten int
	scanMode    scanMode
	st          *dataset.Structure
	wr          io.Writer
	keysWritten map[string]bool
}

// NewJSONWriter creates a Writer from a structure and write destination
func NewJSONWriter(st *dataset.Structure, w io.Writer) (*JSONWriter, error) {
	if st.Schema == nil {
		err := fmt.Errorf("schema required for JSON writer")
		log.Debug(err.Error())
		return nil, err
	}

	jw := &JSONWriter{
		st: st,
		wr: w,
	}

	sm, err := schemaScanMode(st.Schema)
	jw.scanMode = sm
	if sm == smObject {
		jw.keysWritten = map[string]bool{}
	}

	return jw, err
}

// Structure gives this writer's structure
func (w *JSONWriter) Structure() *dataset.Structure {
	return w.st
}

// ContainerType gives weather this writer is writing an array or an object
func (w *JSONWriter) ContainerType() string {
	if w.scanMode == smObject {
		return "object"
	}
	return "array"
}

// WriteEntry writes one JSON record to the writer
func (w *JSONWriter) WriteEntry(ent Entry) error {
	defer func() {
		w.rowsWritten++
	}()
	if w.rowsWritten == 0 {
		open := []byte{'['}
		if w.scanMode == smObject {
			open = []byte{'{'}
		}
		if _, err := w.wr.Write(open); err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error writing initial `%s`: %s", string(open), err.Error())
		}
	}

	data, err := w.valBytes(ent)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	enc := []byte{','}
	if w.rowsWritten == 0 {
		enc = []byte{}
	}

	_, err = w.wr.Write(append(enc, data...))
	return err
}

func (w *JSONWriter) valBytes(ent Entry) ([]byte, error) {
	if w.scanMode == smArray {
		// TODO - add test that checks this is recording values & not entries
		return json.Marshal(ent.Value)
	}

	if ent.Key == "" {
		log.Debug("write empty key")
		return nil, fmt.Errorf("entry key cannot be empty")
	} else if w.keysWritten[ent.Key] == true {
		log.Debugf(`key already written: "%s"`, ent.Key)
		return nil, fmt.Errorf(`key already written: "%s"`, ent.Key)
	}
	w.keysWritten[ent.Key] = true

	data, err := json.Marshal(ent.Key)
	if err != nil {
		log.Debug(err.Error())
		return data, err
	}
	data = append(data, ':')
	val, err := json.Marshal(ent.Value)
	if err != nil {
		log.Debug(err.Error())
		return data, err
	}
	data = append(data, val...)
	return data, nil
}

// Close finalizes the writer, indicating no more records
// will be written
func (w *JSONWriter) Close() error {
	// if WriteEntry is never called, write an empty array
	if w.rowsWritten == 0 {
		data := []byte("[]")
		if w.scanMode == smObject {
			data = []byte("{}")
		}

		if _, err := w.wr.Write(data); err != nil {
			log.Debug(err.Error())
			return fmt.Errorf("error writing empty closure '%s': %s", string(data), err.Error())
		}
		return nil
	}

	cloze := []byte{']'}
	if w.scanMode == smObject {
		cloze = []byte{'}'}
	}
	_, err := w.wr.Write(cloze)
	if err != nil {
		log.Debug(err.Error())
		return fmt.Errorf("error closing writer: %s", err.Error())
	}
	return nil
}
