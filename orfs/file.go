package orfs

import (
	"fmt"
	"os"
)

const BLOCKSIZE int64 = 1024 * 1024 * 4

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
	Inode OBJ
	fs    *Orfs
	pos   uint64
}

func (f *File) Close() error {
	fmt.Fprintf(debuglog, "Close: %v\n", f.Inode.Inode())
	f.pos = 0
	f.fs = nil
	return nil
}

func (f *File) Delete() error {
	return os.ErrInvalid
}

func (f *File) Read(p []byte) (int, error) {
	fmt.Fprintf(debuglog, "Read: %v, pos: %v\n", f.Inode.Inode(), f.pos)
	read, err := f.fs.ioctx.Read(f.Inode.Inode().String(), p, f.pos)
	f.pos += uint64(read)
	return read, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	fmt.Fprintf(debuglog, "Seek: %v, pos: %v, whence: %v\n", f.Inode.Inode(), offset, whence)
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
	fmt.Fprintf(debuglog, "Write: %v\n", f.Inode)
	err := f.fs.ioctx.Write(f.Inode.Inode().String(), p, f.pos)
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
	fmt.Fprintf(debuglog, "Readdir: %v\n", f.Inode.Inode())
	var ret []os.FileInfo
	fsObjList, err := f.Inode.List()
	for _, v := range fsObjList {
		ret = append(ret, v)
	}
	return ret, err
	return nil, fmt.Errorf("Not Implemented directory!")
}

func (f *File) Stat() (os.FileInfo, error) {
	fmt.Fprintf(debuglog, "Stat'ing: %v\n", f.Inode.Inode())
	return f.Inode, nil
}
