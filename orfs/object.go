package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/google/uuid"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

type OBJ interface {
	Name() string
	Rename(string)
	Size() int64
	Mode() os.FileMode
	ModTime() time.Time
	IsDir() bool
	Inode() uuid.UUID
	Sys() interface{}
	Open() (*File, error)
	Unlink(OBJ) error
	FDelete() error
	List() ([]OBJ, error)
	Add(OBJ) error
	Delete(OBJ) error
	HasChild(string) bool
	Get(string) (OBJ, error)
	ReadMD() error
	ReSync() error
}

type fsObj struct {
	name     string
	size     int64
	mode     os.FileMode
	modTime  time.Time
	isDir    bool
	inode    uuid.UUID
	lastRead time.Time
	children map[string]uuid.UUID
	fs       *Orfs
	sync.RWMutex
}

/* FIXME: THIS SHOULDN'T CREATE AN INODE IMMEDIATELY!*/
func NewObj(fs *Orfs, Name string, isDir bool) (OBJ, error) {
	_uuid := uuid.New()
	ctx := fs.mdctx

	if !isDir {
		// Is not directory, write data elsewhere.
		ctx = fs.ioctx
	}
	for {
		_, err := ctx.Stat(_uuid.String())
		if err == nil {
			// UUID already exists
			_uuid = uuid.New()
			continue
		}
		break
	}

	mode := os.FileMode(0644)
	var children map[string]uuid.UUID
	if isDir {
		mode = os.FileMode(0755) | os.ModeDir
		children = make(map[string]uuid.UUID)
	}

	obj := fsObj{
		name:     Name,
		size:     0,
		mode:     mode,
		modTime:  time.Now(),
		isDir:    isDir,
		inode:    _uuid,
		fs:       fs,
		children: children,
	}

	err := obj.ReSync()
	if err != nil {
		return nil, err
	}

	return &obj, nil
}

func GetObjInode(fs *Orfs, Inode uuid.UUID) (OBJ, error) {
	_obj, ok := fs.cache.Get(Inode)
	if !ok {
		fmt.Printf("GetObjInode, Not in cache: %v\n", Inode)
		_obj = &fsObj{
			inode:    Inode,
			fs:       fs,
			children: make(map[string]uuid.UUID),
		}
	}
	obj, ok := _obj.(OBJ)
	if !ok {
		fmt.Printf("Typeof cache obj: %v\n", reflect.TypeOf(_obj))
		fmt.Printf("%+v\n", _obj)
		panic("Not of type fsObj")
	}

	fmt.Printf("GetObjInode: %v\n", obj.Inode())

	err := obj.ReadMD()
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (f *fsObj) Name() string {
	return f.name
}

func (f *fsObj) Rename(name string) {
	f.name = name
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

func (f *fsObj) List() (objList []OBJ, err error) {
	fmt.Printf("Listdir: %+v\n", f.isDir)
	if !f.isDir {
		return nil, os.ErrInvalid
	}
	err = f.ReadMD()
	if err != nil {
		return nil, err
	}

	for _, v := range f.children {
		obj, err := GetObjInode(f.fs, v)
		if err != nil {
			return nil, err
		}
		objList = append(objList, obj)
	}
	return objList, nil
}

func (f *fsObj) Add(o OBJ) error {
	if !f.isDir {
		return os.ErrNotExist
	}

	if o == nil {
		return os.ErrInvalid
	}

	if _, ok := f.children[o.Name()]; ok {
		return os.ErrExist
	}
	// Lock dir
	// Add inode to disk
	// Unlock dir
	f.Lock()
	defer f.Unlock()

	err := o.ReSync()
	if err != nil {
		return err
	}

	err = AddMDEntry(f.fs.mdctx, f.Inode(), '+', o)
	if err != nil {
		return err
	}

	f.children[o.Name()] = o.Inode()

	f.fs.cache.Add(o.Inode(), o)

	return nil
}

func (f *fsObj) Unlink(o OBJ) error {
	f.Lock()
	defer f.Unlock()
	err := AddMDEntry(f.fs.mdctx, f.Inode(), '-', o)
	if err != nil {
		return err
	}
	delete(f.children, o.Name())
	return nil
}

func (f *fsObj) Delete(o OBJ) error {
	if !f.isDir {
		return os.ErrNotExist
	}
	if err := f.Unlink(o); err != nil {
		return err
	}
	err := o.FDelete()
	return err
}

func (f *fsObj) Open() (*File, error) {
	fmt.Fprintf(debuglog, "Open of Inode: %v\n", f.Inode())
	return &File{
		Inode:  f,
		fs:     f.fs,
		pos:    0,
		dInode: f.Inode().String() + ".1", // Stupidly ugly, but works to write and read data.
	}, nil
}

func (f *fsObj) FDelete() error {
	return f.fs.mdctx.Delete(f.Inode().String())
}

func (f *fsObj) HasChild(Name string) bool {
	_, ok := f.children[Name]
	return ok
}

func (f *fsObj) Get(Name string) (OBJ, error) {
	if f.Name() == Name {
		fmt.Printf("Get of name: \"%v\", it's me, returning self\n", Name)
		return f, nil
	}
	for k, v := range f.children {
		fmt.Printf("Cache Get, children: %v: %v\n", k, v)
	}
	Inode, ok := f.children[Name]
	if !ok {
		fmt.Fprintf(debuglog, "Failed to find Inode for: %v in dir: %v\n", Name, f.Inode())
		return nil, os.ErrNotExist
	}
	keys := f.fs.cache.Keys()
	for _, v := range keys {
		fmt.Printf("Cache Get, Key: %v\n", v.(uuid.UUID))
	}
	_obj, ok := f.fs.cache.Get(Inode)
	if !ok {
		fmt.Fprintf(debuglog, "Failed to get Inode %v from cache\n", Inode)
		return nil, os.ErrNotExist
	}
	return _obj.(OBJ), nil
}

func (f *fsObj) ReadMD() error {
	buf := make([]byte, 1024*1024*4) // should make this a loop and parse stuff as i go..
	pos := uint64(0)

	ctx := f.fs.mdctx

	if !f.IsDir() {
		// Is not directory, write data elsewhere.
		ctx = f.fs.ioctx
	}
	fmt.Printf("ReadMD: f.Inode: %+v\n", f.Inode().String())
	stat, err := ctx.Stat(f.Inode().String())
	if err != nil {
		return err
	}
	f.Lock()
	defer f.Unlock()
	if !stat.ModTime.After(f.lastRead) {
		// We already have latest version in memory
		return nil
	}

	for {
		n, err := ctx.Read(f.Inode().String(), buf, pos)
		if err != nil {
			fmt.Fprintf(debuglog, "Failed to read inode: %v, error: %v\n", f.Inode().String(), err)
			return err
		}
		mdEntries := strings.Split(string(buf[:n]), "\n")
		for _, entry := range mdEntries {
			status, stat, err := parseMdEntry([]byte(entry))
			if err == MdEntryEmpty {
				continue
			} else if err != nil {
				fmt.Fprintf(debuglog, "Failed to parse MD entry, entry: %v, error: %v\n", entry, err)
			}
			if status == '+' {
				fmt.Fprintf(debuglog, "Readdir on: %v, adding %v, isdir: %v\n", f.Inode().String(), stat.Name(), stat.IsDir())
				f.children[stat.Name()] = stat.Inode()
				f.fs.cache.Add(stat.Inode(), &fsObj{
					name:     stat.Name(),
					size:     stat.Size(),
					mode:     stat.Mode(),
					modTime:  stat.ModTime(),
					isDir:    stat.IsDir(),
					inode:    stat.Inode(),
					fs:       f.fs,
					children: make(map[string]uuid.UUID),
				})
			} else if status == '-' {
				fmt.Fprintf(debuglog, "Readdir on: %v, removing %v\n", f.Inode().String(), stat.Name())
				delete(f.children, stat.Name())
			} else if status == 'I' {
				f.name = stat.Name()
				f.size = stat.Size()
				f.mode = stat.Mode()
				f.modTime = stat.ModTime()
				f.isDir = stat.IsDir()
			} else {
				return fmt.Errorf("Weird status: %v for entry: %v\n", status, entry)
			}
		}

		if n == len(buf) {
			pos += uint64(n)
		} else {
			break
		}

	}
	f.lastRead = time.Now()
	return nil
}

// Synchronizes the directory to disk.
func (f *fsObj) ReSync() error {
	ctx := f.fs.mdctx

	if !f.IsDir() {
		// Is not directory, write to datapool.
		ctx = f.fs.ioctx
	}

	if f.ModTime().After(f.lastRead) {
		fmt.Printf("ReSync: ModTime is after lastread\n")
		// Stat it, if it exists -> lock it, defer unlock, truncate it.
		_, err := ctx.Stat(f.Inode().String())
		if err == nil {
			// Lock, truncate, unlock
			fmt.Printf("ReSync: Locking Inode\n")
			_, err := ctx.LockExclusive(f.Inode().String(), "Sync", f.Inode().String(), "Sync of dir", 0, nil)
			if err != nil {
				return err
			}
			fmt.Printf("ReSync: Locked Inode\n")
			defer ctx.Unlock(f.Inode().String(), "Sync", f.Inode().String())
		} else if err != rados.RadosErrorNotFound {
			return err
		}
		// With Exclusive lock held, Re-read directory
		if err := f.ReadMD(); err != rados.RadosErrorNotFound {
			return err
		}

		// Create initial "I"node for metadata file
		var md []byte = makeMdEntry('I', f)
		for _, Inode := range f.children {
			obj, err := GetObjInode(f.fs, Inode)
			if err != nil {
				return err
			}
			md = append(md, makeMdEntryNewline('+', obj)...)
		}
		if len(md) == 0 {
			panic("Metadata entry can not be zero!")
		}
		err = ctx.WriteFull(f.Inode().String(), md)
		if err != nil {
			return err
		}
	}

	return nil
}
