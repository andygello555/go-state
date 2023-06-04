package state

import (
	"archive/zip"
	"bytes"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

type FS interface {
	Exists(name string) bool
}

type FSReader interface {
	FS
	fs.FS
}

type FSWriter interface {
	FS
	// WriteFile writes the given data with the given permissions to a file relative to the FSWriter.
	WriteFile(name string, data []byte, perms os.FileMode) error
	// MkdirAll creates all the directories required to
	MkdirAll(path string, perm os.FileMode) error
}

type FSWriteCloser interface {
	FSWriter
	io.Closer
}

type FSReadWriter interface {
	FSReader
	FSWriter
}

type dirFS string

func DirFS(dir string) FSReadWriter {
	return dirFS(dir)
}

func (d dirFS) Exists(name string) bool {
	fullName := filepath.Join(string(d), name)
	_, err := os.Stat(fullName)
	return !os.IsNotExist(err)
}

func (d dirFS) Open(name string) (fs.File, error) {
	fullName := filepath.Join(string(d), name)
	f, err := os.Open(fullName)
	if err != nil {
		err.(*os.PathError).Path = name
		return nil, err
	}
	return f, nil
}

func (d dirFS) WriteFile(name string, data []byte, perms os.FileMode) error {
	fullName := filepath.Join(string(d), name)
	err := os.WriteFile(fullName, data, perms)
	if err != nil {
		err.(*os.PathError).Path = name
		return err
	}
	return nil
}

func (d dirFS) MkdirAll(path string, perm os.FileMode) error {
	fullPath := filepath.Join(string(d), path)
	err := os.MkdirAll(fullPath, perm)
	if err != nil {
		err.(*os.PathError).Path = path
		return err
	}
	return nil
}

type zipFS struct {
	buf     *bytes.Buffer
	paths   map[string]struct{}
	archive *zip.Writer
}

func ZipFS() FSWriteCloser {
	var buf bytes.Buffer
	return &zipFS{
		buf:     &buf,
		paths:   make(map[string]struct{}),
		archive: zip.NewWriter(&buf),
	}
}

func (z *zipFS) Exists(name string) bool {
	_, ok := z.paths[name]
	return ok
}

func (z *zipFS) WriteFile(name string, data []byte, perms os.FileMode) (err error) {
	var w io.Writer
	if w, err = z.archive.Create(name); err != nil {
		err = errors.Wrapf(err, "could not create archived file %q", name)
		return
	}

	if _, err = w.Write(data); err != nil {
		err = errors.Wrapf(err, "could not write %d bytes to file %q in ZipFS", len(data), name)
		return
	}

	// Add the path to the paths set
	z.paths[name] = struct{}{}
	return
}

func (z *zipFS) MkdirAll(path string, perm os.FileMode) error { return nil }
func (z *zipFS) Close() error                                 { return z.archive.Close() }
