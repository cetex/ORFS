package orfs

import (
	"fmt"
	"os"
)

const BLOCKSIZE int64 = 1024 * 1024 * 4

type Inode interface {
	Readdir(count int) ([]os.FileInfo, error)
	Stat() (os.FileInfo, error)
	Read(p []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Write(p []byte) (int, error)
	Close() error
}

type File struct {
	Inode  *fsObj
	dInode string
	fs     *Orfs
	pos    int64
}

func (f *File) Close() error {
	fmt.Fprintf(debuglog, "Close: %v\n", f.Inode.Inode())
	f.Inode.ReSync()
	f.pos = 0
	f.fs = nil
	return nil
}

func (f *File) Read(p []byte) (int, error) {
	fmt.Fprintf(debuglog, "Read: %v, pos: %v\n", f.Inode.Inode(), f.pos)
	read, err := f.fs.ioctx.Read(f.dInode, p, uint64(f.pos)) // FIXME, this is stupidly ugly, we should stripe data over multiple blocks.
	fmt.Printf("File Read: GOT DATA: %v\n", p)
	f.pos += int64(read)
	return read, err
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	fmt.Fprintf(debuglog, "Seek: %v, pos: %v, whence: %v\n", f.dInode, offset, whence)
	switch whence {
	case 0: // Seek from start of file
		f.pos = offset
	case 1: // Seek from current position
		f.pos += offset
	case 2: // Seek from end of file
		stat, err := f.Stat()
		if err != nil {
			return int64(f.pos), fmt.Errorf("Failed to get current object size")
		}
		f.pos = stat.Size() + offset
	}
	return int64(f.pos), nil
}

func (f *File) Write(p []byte) (int, error) {
	fmt.Fprintf(debuglog, "Write: %v\n", f.Inode)
	err := f.fs.ioctx.Write(f.dInode, p, uint64(f.pos))
	if err != nil {
		// If error, assume nothing was written. Ceph should be fully
		// consistent and if write fails without info on how much was
		// written, we have to assume it was aborted.
		return 0, err
	}
	f.pos += int64(len(p))
	if f.pos > f.Inode.size {
		f.Inode.size = f.pos
	}
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
}

func (f *File) Stat() (os.FileInfo, error) {
	fmt.Fprintf(debuglog, "Stat'ing: %v\n", f.Inode.Inode())
	return f.Inode, nil
}
