package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/google/uuid"
	"os"
	"sync"
	//	"strings"
	"time"
)

type DIR interface {
	Name() string
	Size() int64
	Mode() os.FileMode
	ModTime() time.Time
	IsDir() bool
	Inode() uuid.UUID
	Sys() interface{}
	ListDir() ([]DIR, error)
	AddDir(DIR) error
	DeleteDir(DIR) error
	ListObj() ([]OBJ, error)
	AddObj(OBJ) error
	DeleteObj(OBJ) error
	ReSync() error
}

type OBJ interface {
	Name() string
	Size() int64
	Mode() os.FileMode
	ModTime() time.Time
	IsDir() bool
	Inode() uuid.UUID
	Sys() interface{}
	Open() (*File, error)
	Delete() error
}

type fsObj struct {
	name     string
	size     int64
	mode     os.FileMode
	modTime  time.Time
	isDir    bool
	inode    uuid.UUID
	parent   *DIR
	lastRead time.Time
	childDir map[uuid.UUID]DIR
	childObj map[uuid.UUID]OBJ
	fs       *Orfs
	sync.RWMutex
}

func NewDir(fs *Orfs, Name string) (DIR, error) {
	_uuid := uuid.New()
	for {
		_, err := fs.mdctx.Stat(_uuid.String())
		if err == nil {
			// UUID already exists
			_uuid = uuid.New()
			continue
		}
		break
	}

	dir := fsObj{
		name:     Name,
		size:     0,
		mode:     os.FileMode(755),
		modTime:  time.Now(),
		isDir:    true,
		inode:    _uuid,
		fs:       fs,
		childDir: make(map[uuid.UUID]DIR),
		childObj: make(map[uuid.UUID]OBJ),
	}

	err := fs.mdctx.WriteFull(dir.Inode().String(), makeMdEntry('+', &dir))
	if err != nil {
		return nil, err
	}

	return &dir, nil
}

func (f *fsObj) Name() string {
	return f.name
}

func (f *fsObj) Size() int64 {
	return f.size
}

func (f *fsObj) Mode() os.FileMode {
	return f.mode
}

func (f *fsObj) ModTime() time.Time {
	return f.modTime
}

func (f *fsObj) IsDir() bool {
	return f.isDir
}

func (f *fsObj) Inode() uuid.UUID {
	return f.inode
}

func (f *fsObj) Sys() interface{} {
	return nil
}

func (f *fsObj) ListDir() (dirList []DIR, err error) {
	fmt.Printf("Listdir: %+v\n", f.isDir)
	if !f.isDir {
		return nil, os.ErrInvalid
	}
	stat, err := f.fs.mdctx.Stat(uuid.UUID(f.Inode()).String())
	if err != nil {
		return nil, err
	}
	f.RLock()
	defer f.RUnlock()
	if stat.ModTime.After(f.lastRead) {
		err = f.ReadMD()
		if err != nil {
			return nil, err
		}
	}

	for _, v := range f.childDir {
		dirList = append(dirList, v)
	}
	return dirList, nil
}

func AddMDEntry(mdctx *rados.IOContext, DirInode uuid.UUID, action byte, obj OrfsStat) error {
	_, err := mdctx.LockExclusive(DirInode.String(), "AddEntry", obj.Inode().String(), "Lock for entry addition", 0, nil)
	if err != nil {
		return err
	}
	defer mdctx.Unlock(DirInode.String(), "AddEntry", obj.Inode().String())
	fmt.Printf("Adding obj to metadata: %v\n", obj)
	err = mdctx.Append(DirInode.String(), makeMdEntryNewline(action, obj))
	if err != nil {
		return err
	}
	return nil
}

func (f *fsObj) AddDir(d DIR) error {
	if !f.isDir {
		return os.ErrNotExist
	}

	if d == nil {
		return os.ErrInvalid
	}
	// Lock dir
	// Add inode to disk
	// Unlock dir
	f.Lock()
	defer f.Unlock()
	err := AddMDEntry(f.fs.mdctx, f.Inode(), '+', d)
	if err != nil {
		return err
	}
	f.childDir[d.Inode()] = d
	return nil
}

func (f *fsObj) DeleteDir(d DIR) error {
	if !f.isDir {
		return os.ErrNotExist
	}
	f.Lock()
	defer f.Unlock()
	err := AddMDEntry(f.fs.mdctx, f.Inode(), '-', d)
	if err != nil {
		return err
	}
	delete(f.childDir, d.Inode())
	return nil
}

func (f *fsObj) ListObj() (objList []OBJ, err error) {
	if !f.isDir {
		return nil, os.ErrNotExist
	}
	f.RLock()
	defer f.RUnlock()
	for _, v := range f.childObj {
		objList = append(objList, v)
	}
	return objList, nil
}

func (f *fsObj) AddObj(o OBJ) error {
	if !f.isDir {
		return os.ErrNotExist
	}
	if o == nil {
		return os.ErrInvalid
	}
	f.Lock()
	defer f.Unlock()
	err := AddMDEntry(f.fs.mdctx, f.Inode(), '+', o)
	if err != nil {
		return err
	}
	f.childObj[o.Inode()] = o
	return nil
}

func (f *fsObj) DeleteObj(o OBJ) error {
	if !f.isDir {
		return os.ErrNotExist
	}
	f.Lock()
	defer f.Unlock()
	err := AddMDEntry(f.fs.mdctx, f.Inode(), '-', o)
	if err != nil {
		return err
	}
	delete(f.childObj, o.Inode())
	return nil
}

func (f *fsObj) Open() (*File, error) {
	if f.isDir {
		return nil, os.ErrInvalid
	}
	// To be implemented, should return file handle
	return nil, nil
}

func (f *fsObj) Delete() error {
	if f.isDir {
		return os.ErrInvalid
	}
	// To be implemented, should call delete on data.
	// maybe "file" should be inode?
	return nil
}

// Synchronizes the directory to disk.
func (f *fsObj) ReSync() error {
	if f.ModTime().After(f.lastRead) {
		// Stat it, if it exists -> lock it, defer unlock, truncate it.
		_, err := f.fs.mdctx.Stat(f.Inode().String())
		if err == nil {
			// Lock, truncate, unlock
			_, err := f.fs.mdctx.LockExclusive(f.Inode().String(), "Sync", f.Inode().String(), "Sync of dir", 0, nil)
			if err != nil {
				return err
			}
			defer f.fs.mdctx.Unlock(f.Inode().String(), "Sync", f.Inode().String())
			f.Lock()
			defer f.Unlock()
		} else if err != rados.RadosErrorNotFound {
			return err
		}
		// With Exclusive lock held, Re-read directory
		if err := f.ReadMD(); err != rados.RadosErrorNotFound {
			return err
		}

		var md []byte
		for _, _dir := range f.childDir {
			md = append(md, makeMdEntryNewline('+', _dir)...)
		}
		for _, _obj := range f.childObj {
			md = append(md, makeMdEntryNewline('+', _obj)...)
		}
		err = f.fs.mdctx.WriteFull(f.Inode().String(), md)
		if err != nil {
			return err
		}
	}

	return nil
}
