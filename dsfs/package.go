package dsfs

import (
	"github.com/ipfs/go-datastore"
	"path/filepath"
	"strings"

	"github.com/qri-io/cafs"
)

// PackageFile specifies the different types of files that are
// stored in a package
type PackageFile int

const (
	// PackageFileUnknown is the default package file, which
	// should be erroneous, as there is no sensible default
	// for PackageFile
	PackageFileUnknown PackageFile = iota
	// PackageFileDataset is the maind dataset.json file
	// that contains all dataset metadata, and is the only
	// required file to constitute a dataset
	PackageFileDataset
	// PackageFileStructure isolates this dataset's structure
	// in it's own file
	PackageFileStructure
	// PackageFileAbstract is the abstract verion of
	// structure
	PackageFileAbstract
	// PackageFileResources lists the resource datasets
	// that went into creating a dataset
	// TODO - I think this can be removed now that Transform exists
	PackageFileResources
	// PackageFileCommit isolates the user-entered
	// documentation of the changes to this dataset's history
	PackageFileCommit
	// PackageFileTransform isloates the concrete transform that
	// generated this dataset
	PackageFileTransform
	// PackageFileAbstractTransform is the abstract version of
	// the operation performed to create this dataset
	PackageFileAbstractTransform
	// PackageFileMeta encapsulates human-readable metadata
	PackageFileMeta
	// PackageFileVisConfig isolates the data related to representing a dataset as a visualization
	PackageFileVisConfig
)

// filenames maps PackageFile to their filename counterparts
var filenames = map[PackageFile]string{
	PackageFileUnknown:           "",
	PackageFileDataset:           "dataset.json",
	PackageFileStructure:         "structure.json",
	PackageFileAbstract:          "abstract.json",
	PackageFileAbstractTransform: "abstract_transform.json",
	PackageFileResources:         "resources",
	PackageFileCommit:            "commit.json",
	PackageFileTransform:         "transform.json",
	PackageFileMeta:              "meta.json",
	PackageFileVisConfig:         "vis_config.json",
}

// String implements the io.Stringer interface for PackageFile
func (p PackageFile) String() string {
	return p.Filename()
}

// Filename gives the canonical filename for a PackageFile
func (p PackageFile) Filename() string {
	return filenames[p]
}

// PackageFilepath relies on package storage conventions and cafs.Filestore path prefixes
// to deliver the path to a package file for a given base path
func PackageFilepath(store cafs.Filestore, path string, pf PackageFile) string {
	switch store.PathPrefix() {
	case "ipfs":
		return filepath.Join("/ipfs", ipfsHashBase(path), pf.String())
	default:
		return path
	}
}

// ipfsHashBase strips paths to return just the hash
func ipfsHashBase(in string) string {
	in = strings.TrimLeft(in, "/")
	in = strings.TrimPrefix(in, "ipfs/")
	return strings.Split(in, "/")[0]
}

// PackageKeypath wraps PackageFilepath to work with datastore.Keys instead
func PackageKeypath(store cafs.Filestore, path datastore.Key, pf PackageFile) datastore.Key {
	return datastore.NewKey(PackageFilepath(store, path.String(), pf))
}
