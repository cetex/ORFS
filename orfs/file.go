package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"log"
	"os"
)

type Inode interface {
	Readdir(count int) ([]os.FileInfo, error) // How do i implement this properly?
	Stat() (os.FileInfo, error)
	Read(p []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Write(p []byte) (int, error)
	Close() error
	Delete() error // Delete / Unlink the data.
}

type File struct {
	oid  string
	pos  uint64
	orfs *Orfs
}

func (f *File) Close() error {
	fmt.Fprintf(f.orfs.debuglog, "Close: %v\n", f.oid)
	f.oid = ""
	f.pos = 0
	f.orfs = nil
	return nil
}

func (f *File) Read(p []byte) (int, error) {
	fmt.Fprintf(f.orfs.debuglog, "Read: %v\n", f.oid)
	read, err := f.orfs.ioctx.Read(f.oid, p, f.pos)
	f.pos += uint64(read)
	return read, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	fmt.Fprintf(f.orfs.debuglog, "Seek: %v\n", f.oid)
	switch whence {
	case 0: // Seek from start of file
		f.pos = uint64(offset)
	case 1: // Seek from current position
		f.pos += uint64(offset)
	case 2: // Seek from end of file
		stat, err := f.Stat()
		if err != nil {
			return int64(f.pos), fmt.Errorf("Failed to get current object size")
		}
		f.pos = uint64(stat.Size() + offset)
	}
	return int64(f.pos), nil
}

func (f *File) Write(p []byte) (int, error) {
	fmt.Fprintf(f.orfs.debuglog, "Write: %v\n", f.oid)
	err := f.orfs.ioctx.Write(f.oid, p, f.pos)
	if err != nil {
		// If error, assume nothing was written. Ceph should be fully
		// consistent and if write fails without info on how much was
		// written, we have to assume it was aborted.
		return 0, err
	}
	f.pos += uint64(len(p))
	return len(p), nil
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	fmt.Fprintf(f.orfs.debuglog, "Readdir: %v\n", f.oid)
	return nil, fmt.Errorf("Not Implemented directory!")
}

func (f *File) Stat() (os.FileInfo, error) {
	log.Println("Stat: ", f.oid)
	fmt.Printf("Stat'ing: %v\n", f.oid)
	stat, err := f.orfs.mdctx.Stat(f.oid)
	fmt.Printf("stat: %+v, err: %v\n", stat, err)
	if err != nil {
		log.Printf("Failed mdctx.Stat oid: %v, err: %v\n", f.oid, err)
		spew.Dump(err)
		switch err {
		case rados.RadosErrorNotFound:
			return nil, os.ErrNotExist
		default:
			return nil, err
		}
	}
	s := &Istat{
		name:    f.oid,
		size:    int64(stat.Size),
		mode:    os.FileMode(0644),
		modTime: stat.ModTime,
		isDir:   false,
		inode:   uuid.New(),
		sys:     nil,
	}
	str := makeMdEntry('+', s)
	n, err := fmt.Printf("MD Entry: Len:%v, %+v\n", len(str), string(str))
	fmt.Printf("Printed: %v chars\n", n)
	if err != nil {
		panic(err)
	}
	state, entry, err := parseMdEntry(str)
	if err != nil {
		panic(err)
	}
	str = makeMdEntry(state, entry)
	n, err = fmt.Printf("Recreated MD Entry: Len:%v, %+v\n", len(str), string(str))
	fmt.Printf("Printed: %v chars\n", n)
	if err != nil {
		panic(err)
	}

	return s, nil
}
