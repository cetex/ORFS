package orfs

import (
	"os"
	"time"
)

type cephStat struct {
	name     string
	size     int64
	mode     os.FileMode
	modTime  time.Time
	isDir    bool
	sys      interface{}
	statDone bool
}

func (s *cephStat) Name() string {
	return s.name
}

func (s *cephStat) Size() int64 {
	return s.size
}

func (s *cephStat) Mode() os.FileMode {
	return s.mode
}

func (s *cephStat) ModTime() time.Time {
	return s.modTime
}

func (s *cephStat) IsDir() bool {
	return s.isDir
}

func (s *cephStat) Sys() interface{} {
	return s.sys
}
