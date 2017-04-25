package orfs

import (
	"github.com/google/uuid"
	"os"
	"time"
)

type OrfsStat interface {
	os.FileInfo
	Inode() uuid.UUID
}

type Istat struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
	inode   uuid.UUID
}

func (s *Istat) Name() string {
	return s.name
}

func (s *Istat) Size() int64 {
	return s.size
}

func (s *Istat) Mode() os.FileMode {
	return s.mode
}

func (s *Istat) ModTime() time.Time {
	return s.modTime
}

func (s *Istat) IsDir() bool {
	return s.isDir
}

func (s *Istat) Inode() uuid.UUID {
	return s.inode
}

func (s *Istat) Sys() interface{} {
	return s.sys
}
