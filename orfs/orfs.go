package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

var log io.Writer = ioutil.Discard
var debuglog io.Writer = ioutil.Discard

type Orfs struct {
	conn   *rados.Conn
	ioctx  *rados.IOContext
	mdctx  *rados.IOContext
	pool   string
	mdpool string
	Root   OBJ
	cache  lru.Cache
}

// Creates a new instance of ORFS
// pool is the datapool and mdpool is the metadatapool
// Both pools can be the same pool as long as the pool supports
// partial writes, an erasure coded pool is not supported for
// metadata.
//
// Example:
//  datapool := "test"
//  metadatapool := "test-metadata"
//  cachesize := 1024*1024 // number of metadata entries in cache
//  fs := NewORFS(datapool, metadatapool, cacheSize)
func NewORFS(pool, mdpool string, cacheSize int) *Orfs {
	c := new(Orfs)
	c.pool = pool
	c.mdpool = mdpool
	cache, err := lru.New(cacheSize)
	if err != nil {
		panic(err)
	}
	c.cache = *cache
	return c
}

// Sets the log output, default is ioutil.discard
//
// Example:
// fs := NewORFS(pool, mdpool)
// fs.SetLog(os.Stdout)
func (fs *Orfs) SetLog(slog io.Writer) {
	log = slog
}

// Sets the debuglog output, default is ioutil.discard
//
// Example:
//  fs := NewORFS(pool, mdpool, cachesize)
//  fs.SetDebugLog(os.Stdout)
func (fs *Orfs) SetDebugLog(dlog io.Writer) {
	debuglog = dlog
}

func (fs *Orfs) getRootDir() (OBJ, error) {
	rootUUID := uuid.UUID{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}

	root := fsObj{
		name:     "/",
		size:     0,
		mode:     os.FileMode(755) | os.ModeDir,
		modTime:  time.Now(),
		isDir:    true,
		inode:    rootUUID,
		fs:       fs,
		children: make(map[string]uuid.UUID),
	}

	//root.Add(&root)
	err := root.ReSync()
	return &root, err

}

// Connect to Ceph
//
// Example:
//  fs := NewORFS(pool, mdpool, cachesize)
//  err := fs.Connect()
//  if err != nil {
//      panic(err)
//  }
func (fs *Orfs) Connect() error {
	fmt.Fprint(debuglog, "Connect: Creating connection\n")
	if conn, err := rados.NewConn(); err != nil {
		fmt.Fprintf(debuglog, "ERROR: Connect: NewConn: %v\n", err)
		return err
	} else {
		fs.conn = conn
	}
	fmt.Fprint(debuglog, "Conncet: Reading config file\n")
	if err := fs.conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprintf(debuglog, "ERROR: Connect: ReadConfig: %v\n", err)
		return err
	}
	fmt.Fprint(debuglog, "Connect: Connecting to ceph\n")
	if err := fs.conn.Connect(); err != nil {
		fmt.Fprintf(debuglog, "ERROR: Connect: Connect: %v\n", err)
		return err
	}
	fmt.Fprintf(debuglog, "Connect: Creating IO Context for pool: %v\n", fs.pool)
	if ioctx, err := fs.conn.OpenIOContext(fs.pool); err != nil {
		fmt.Fprintf(debuglog, "ERROR: Connect: OpenIOContext: %v\n", err)
		return err
	} else {
		fs.ioctx = ioctx
	}
	fmt.Fprintf(debuglog, "Connect: Creating IO context for pool: %v\n", fs.mdpool)
	if mdctx, err := fs.conn.OpenIOContext(fs.mdpool); err != nil {
		fmt.Fprintf(debuglog, "ERROR: Connect: OpenIOContext: %v\n", err)
		return err
	} else {
		fs.mdctx = mdctx
	}

	fmt.Fprintf(debuglog, "Connect: Initialized\n")
	fmt.Fprintf(debuglog, "Connect: Loading rootdir\n")
	root, err := fs.getRootDir()
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(log, "Loaded rootdir\n")
	fs.Root = root
	return nil
}

func pathSplit(path string) []string {
	fpath := strings.Split(path, "/")
	fmt.Println("Path: ", fpath)
	// Delete empty strings after the split.. Can we make this more efficient?
	for i := 0; i < len(fpath); i++ {
		if fpath[i] == "" {
			fpath = append(fpath[:i], fpath[i+1:]...)
			i--
		}
	}
	fmt.Println("Path After cleanup: ", fpath)
	return fpath
}

// Get an object (File, Directory) from ORFS.
// name is the path, for example /testdir/testfile
// If GetParent is set it returns the parent of testfile.
//
// Example:
//  fs := NewORFS(pool, mdpool, cachesize)
//  err := fs.Connect()
//  if err != nil {
//      panic(err)
//  }
//
//  obj, err := fs.GetObject("/",  false)
//  obj is the object for root, "/"
//  obj, err := fs.GetObject("/testdir/testfile", false)
//  obj is the testfile
//  obj, err := fs.GetObject("/testdir/testfile", true)
//  obj is the directory "testdir"
func (fs *Orfs) GetObject(name string, GetParent bool) (OBJ, error) {
	dir := fs.Root
	path := pathSplit(name)

	if len(path) == 0 {
		// Path is empty, is root
		return fs.Root, nil
	}

	skip := 0
	if GetParent {
		skip = 1
	}

	// Call update on parentObject to populate children if it hasn't happened yet.
	for i := 0; i < len(path)-skip; i++ {
		fmt.Fprintf(debuglog, "FindObject: current path: %v, i: %v, len(path): %v\n", path[i], i, len(path))
		if _dir, err := dir.Get(path[i]); err == nil {
			fmt.Fprintf(debuglog, "FindObject: Found child: %v\n", _dir.Name())
			dir = _dir
		} else {
			fmt.Fprintf(debuglog, "FindObject: Couldn't find parent object\n")
			// Parent object doesn't exist
			return nil, os.ErrNotExist
		}
	}
	return dir, nil
}

// Create a directory in ORFS.
// name is the path, for example /test/NewDir
// The parent directory "/test" must exist.
//
// Example:
//  fs := NewORFS(pool, mdpool, cachesize)
//  err := fs.Connect()
//  if err != nil {
//      panic(err)
//  }
//  err := fs.Mkdir("/test", os.FileMode(0755))
//  if err != nil {
//      return(err)
//  }
func (fs *Orfs) Mkdir(name string, perm os.FileMode) error {
	fmt.Fprintf(debuglog, "Mkdir: %v\n", name)

	dir, err := fs.GetObject(name, true)
	if err != nil {
		return err
	}

	path := pathSplit(name)
	subdir, err := NewDir(fs, path[len(path)-1:][0])
	if err != nil {
		return err
	}
	err = dir.Add(subdir)
	return err
}

// Open a file or directory in ORFS.
// Works the same as os.OpenFile
// name is the path, for example /test/NewDir or /test/testfile
//
// Example:
//  fs := NewORFS(pool, mdpool, cachesize)
//  err := fs.Connect()
//  if err != nil {
//      panic(err)
//  }
//  file, err := fs.OpenFile("/test/testfile", 0, os.FileMode(0755))
//  if err != nil {
//      return(err)
//  }
func (fs *Orfs) OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	fmt.Fprintf(debuglog, "OpenFile: %v, flag: %v, perm: %v\n", name, flag, perm)
	//	path := pathSplit(name)
	//	dir, err := fs.FindObject(path)
	//	if err != nil {
	//		return nil, err
	//	}
	//	obj, err := dir.Get(path[len(path)-1:][0])
	obj, err := fs.GetObject(name, false)
	if err != nil {
		return nil, err
	}
	return obj.Open()
}

// Remove an object
func (fs *Orfs) RemoveAll(name string) error {
	fmt.Fprintf(debuglog, "Removeall: %v\n", name)
	path := pathSplit(name)
	dir, err := fs.GetObject(name, true)
	if err != nil {
		return err
	}
	obj, err := dir.Get(path[len(path)-1:][0])
	if err != nil {
		return err
	}
	err = dir.Delete(obj)
	return err

}

// Rename an Object
func (fs *Orfs) Rename(oldName, newName string) error {
	fmt.Fprintf(debuglog, "Rename: oldName: %v, newName: %v\n", oldName, newName)
	// Find old dir
	oldDir, err := fs.GetObject(oldName, true)
	if err != nil {
		return err
	}
	// Grab object from dir
	path := pathSplit(oldName)
	obj, err := oldDir.Get(path[len(path)-1:][0])
	if err != nil {
		return err
	}
	// Find new dir
	newDir, err := fs.GetObject(newName, true)
	if err != nil {
		return err
	}
	// Rename obj
	obj.Rename(path[len(path)-1:][0])

	// Link obj object to new dir
	err = newDir.Add(obj)
	if err != nil {
		return err
	}
	// Unlink obj from old dir
	err = oldDir.Unlink(obj)
	if err != nil {
		return err
	}
	return nil
}

// Stat an object
func (fs *Orfs) Stat(name string) (os.FileInfo, error) {
	fmt.Fprintf(debuglog, "Stat: %v\n", name)
	return fs.GetObject(name, false)

}
