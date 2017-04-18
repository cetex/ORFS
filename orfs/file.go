package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/davecgh/go-spew/spew"
	"log"
	"os"
	"time"
)

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
	if f.oid == "" {
		// Is root directory, create file listing.
		dirList := []os.FileInfo{}
		if root, err := f.rootStat(); err != nil {
			return nil, err
		} else {
			dirList = append(dirList, root)
		}
		iter, err := f.orfs.ioctx.Iter()
		if err != nil {
			return nil, err
		}
		defer iter.Close()
		for iter.Next() {
			log.Printf("%v\n", iter.Value())
			stat, err := f.orfs.Stat(iter.Value())
			if err != nil {
				fmt.Fprintf(f.orfs.debuglog, "Error in Readdir / stat: %v", err)
			}
			dirList = append(dirList, stat)
		}
		return dirList, iter.Err()
	}
	return nil, fmt.Errorf("Not a directory!")
}

func (f *File) rootStat() (*Stat, error) {
	stat, err := f.orfs.ioctx.GetPoolStats()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &Stat{
		name:    "",
		size:    int64(stat.Num_bytes),
		mode:    os.FileMode(755) + 1<<(32-1),
		modTime: time.Now(),
		isDir:   true,
		sys:     nil,
	}, nil
}

func (f *File) Stat() (os.FileInfo, error) {
	log.Println("Stat: ", f.oid)
	if f.oid == "" {
		// Stat on root directory, make up a directory..
		return f.rootStat()
	}
	stat, err := f.orfs.ioctx.Stat(f.oid)
	if err != nil {
		log.Println(err)
		spew.Dump(err)
		switch err {
		case rados.RadosErrorNotFound:
			return nil, os.ErrNotExist
		default:
			return nil, err
		}
	}
	return &Stat{
		name:    f.oid,
		size:    int64(stat.Size),
		mode:    os.FileMode(0644),
		modTime: stat.ModTime,
		isDir:   false,
		sys:     nil,
	}, nil
}
