package state

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io/fs"
)

type EncodingType string

const (
	JSON EncodingType = ".json"
	Gob  EncodingType = ".bin"
)

func (et EncodingType) String() string {
	switch et {
	case JSON:
		return "JSON"
	case Gob:
		return "Gob"
	default:
		return "<nil>"
	}
}

// Ext returns the appropriate file extension for the given EncodingType.
func (et EncodingType) Ext() string { return string(et) }

func serialiseJSON(state StateWriter, key *hashKey, value any) (err error) {
	var data []byte
	path := key.path()
	if data, err = json.Marshal(&value); err != nil {
		return errors.Wrapf(err, "cannot serialise value for key %s to JSON", path)
	}
	return state.FSWriter().WriteFile(path, data, filePerms)
}

func serialiseGob(state StateWriter, key *hashKey, value any) (err error) {
	var data bytes.Buffer
	path := key.path()
	if err = gob.NewEncoder(&data).Encode(&value); err != nil {
		err = errors.Wrapf(err, "cannot serialise value for key %s to Gob", path)
		return
	}
	return state.FSWriter().WriteFile(path, data.Bytes(), filePerms)
}

func (et EncodingType) serialise(state StateWriter, key *hashKey, value any) (err error) {
	switch et {
	case JSON:
		return serialiseJSON(state, key, value)
	case Gob:
		return serialiseGob(state, key, value)
	default:
		return fmt.Errorf("cannot serialise value for hash key %v to disk using encoding %s", key, string(et))
	}
}

func deserialiseJSON(state StateReader, key *hashKey, value any) (err error) {
	var file fs.File
	path := key.path()
	if file, err = state.FSReader().Open(path); err != nil {
		err = errors.Wrapf(err, "file for key %s could not be opened", path)
		return
	}

	if err = json.NewDecoder(file).Decode(value); err != nil {
		err = errors.Wrapf(err, "could not deserialise value for key %s from JSON", path)
	}
	return
}

func deserialiseGob(state StateReader, key *hashKey, value any) (err error) {
	var file fs.File
	path := key.path()
	if file, err = state.FSReader().Open(path); err != nil {
		err = errors.Wrapf(err, "file for key %s could not be opened", path)
		return
	}

	if err = gob.NewDecoder(file).Decode(value); err != nil {
		err = errors.Wrapf(err, "could not deserialise value for key %s from Gob", path)
	}
	return
}

func (et EncodingType) deserialise(state StateReader, key *hashKey, value any) (err error) {
	switch et {
	case JSON:
		return deserialiseJSON(state, key, value)
	case Gob:
		return deserialiseGob(state, key, value)
	default:
		return fmt.Errorf("cannot deserialise hash key %v from disk using encoding %s", key, string(et))
	}
}
