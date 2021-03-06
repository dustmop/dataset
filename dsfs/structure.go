package dsfs

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
)

// SaveStructure saves a query's structure to a given store
func SaveStructure(store cafs.Filestore, s *dataset.Structure, pin bool) (path datastore.Key, err error) {
	file, err := JSONFile(PackageFileStructure.String(), s)
	if err != nil {
		log.Debug(err.Error())
		return datastore.NewKey(""), fmt.Errorf("error saving json structure file: %s", err.Error())
	}
	return store.Put(file, pin)
}

// LoadStructure loads a structure from a given path in a store
func LoadStructure(store cafs.Filestore, path datastore.Key) (st *dataset.Structure, err error) {
	path = PackageKeypath(store, path, PackageFileStructure)
	return loadStructure(store, path)
}

// loadStructure assumes path is valid
func loadStructure(store cafs.Filestore, path datastore.Key) (st *dataset.Structure, err error) {
	data, err := fileBytes(store.Get(path))
	if err != nil {
		log.Debug(err.Error())
		return nil, fmt.Errorf("error loading structure file: %s", err.Error())
	}
	return dataset.UnmarshalStructure(data)
}
