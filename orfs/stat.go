package orfs

import (
	"os"
	"time"
)

type Stat struct {
	name     string
	size     int64
	mode     os.FileMode
	modTime  time.Time
	isDir    bool
	sys      interface{}
	statDone bool
}

func (s *Stat) Name() string {
	return s.name
}

func (s *Stat) Size() int64 {
	return s.size
}

func (s *Stat) Mode() os.FileMode {
	return s.mode
}

func (s *Stat) ModTime() time.Time {
	return s.modTime
}

func (s *Stat) IsDir() bool {
	return s.isDir
}

func (s *Stat) Sys() interface{} {
	return s.sys
}
