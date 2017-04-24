package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"os"
	"time"
)

type Orfs struct {
	conn     *rados.Conn
	ioctx    *rados.IOContext
	mdctx    *rados.IOContext
	pool     string
	mdpool   string
	log      io.Writer
	debuglog io.Writer
	Root     DIR
}

// Creates a new instance of ORFS
// pool is the datapool and mdpool is the metadatapool
// Both pools can be the same pool as long as the pool supports
// partial writes, an erasure coded pool is not supported for
// metadata.
//
// Example:
// datapool := "test"
// metadatapool := "test-metadata"
// fs := NewORFS(datapool, metadatapool)
func NewORFS(pool, mdpool string) *Orfs {
	c := new(Orfs)
	c.pool = pool
	c.mdpool = mdpool
	c.log = ioutil.Discard      // Default to discarding all logs
	c.debuglog = ioutil.Discard // Default to discarding all debug logs
	return c
}

// Sets the log output, default is ioutil.discard
//
// Example:
// fs := NewORFS(pool, mdpool)
// fs.SetLog(os.Stdout)
func (c *Orfs) SetLog(log io.Writer) {
	c.log = log
}

// Sets the debuglog output, default is ioutil.discard
//
// Example:
// fs := NewORFS(pool, mdpool)
// fs.SetDebugLog(os.Stdout)
func (c *Orfs) SetDebugLog(debugLog io.Writer) {
	c.debuglog = debugLog
}

//func (c *Orfs) NewDir(Name string) (DIR, error) {
//	Inode := uuid.New()
//	for {
//		_, err := c.mdctx.Stat(Inode.String())
//		if err == nil {
//			return nil, os.ErrExist
//		}
//	}
//	dir := fsObj{
//		name:     Name,
//		size:     0,
//		mode:     os.FileMode(0755),
//		modTime:  time.Now(),
//		isDir:    true,
//		inode:    Inode,
//		fs:       c,
//		childDir: make(map[uuid.UUID]DIR),
//		childObj: make(map[uuid.UUID]OBJ),
//	}
//
//	err = c.mdctx.WriteFull(Inode.String(), makeMdEntry('+', &dir))
//	if err != nil {
//		return nil, err
//	}
//
//	return &dir, nil
//}

func (c *Orfs) GetRootDir() (DIR, error) {
	rootUUID := uuid.UUID{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}

	root := fsObj{
		name:     ".",
		size:     0,
		mode:     os.FileMode(755),
		modTime:  time.Now(),
		isDir:    true,
		inode:    rootUUID,
		fs:       c,
		childDir: make(map[uuid.UUID]DIR),
		childObj: make(map[uuid.UUID]OBJ),
	}

	root.AddDir(&root)
	err := root.ReSync()
	return &root, err

}

// Connect to Ceph
//
// Example:
// fs := NewORFS(pool, mdpool)
// err := fs.Connect()
// if err != nil {
//     panic(err)
// }
func (c *Orfs) Connect() error {
	fmt.Fprint(c.debuglog, "Creating connection\n")
	if conn, err := rados.NewConn(); err != nil {
		fmt.Fprint(c.debuglog, err)
		return err
	} else {
		c.conn = conn
	}
	fmt.Fprint(c.debuglog, "Reading config file\n")
	if err := c.conn.ReadDefaultConfigFile(); err != nil {
		fmt.Fprint(c.debuglog, err)
		return err
	}
	fmt.Fprint(c.debuglog, "Connecting to ceph\n")
	if err := c.conn.Connect(); err != nil {
		fmt.Fprint(c.debuglog, err)
		return err
	}
	fmt.Fprintf(c.debuglog, "Creating IO Context for pool: %v\n", c.pool)
	if ioctx, err := c.conn.OpenIOContext(c.pool); err != nil {
		fmt.Fprintf(c.debuglog, "Failed to create iocontext for pool, does the pool exist?: %v\n", err)
		return err
	} else {
		c.ioctx = ioctx
	}
	fmt.Fprintf(c.debuglog, "Creating IO context for pool: %v\n", c.mdpool)
	if mdctx, err := c.conn.OpenIOContext(c.mdpool); err != nil {
		fmt.Fprintf(c.debuglog, "Failed to create iocontext for pool, does the pool exist?: %v\n", err)
		return err
	} else {
		c.mdctx = mdctx
	}

	fmt.Fprint(c.debuglog, "Initialized\n")
	fmt.Fprintf(c.debuglog, "Loading rootdir\n")
	root, err := c.GetRootDir()
	if err != nil {
		panic(err)
	}
	c.Root = root
	return nil
}

//
//func (fs *fsDir) GetChildrenOsFileInfo() []os.FileInfo {
//	fileInfo := []os.FileInfo{}
//	if fs.children == nil {
//		return []os.FileInfo{}
//	}
//	for _, v := range fs.children {
//		fileInfo = append(fileInfo, v.stat)
//	}
//	return fileInfo
//}
//
//func pathSplit(path string) []string {
//	fpath := strings.SplitAfter(path, "/")
//	// Delete empty strings after the split.. Can we make this more efficient?
//	for i := 1; i < len(fpath); i++ {
//		if fpath[i] == "" {
//			fpath = append(fpath[:i], fpath[i+1:]...)
//			i--
//		}
//	}
//	return fpath
//}
//
//func (fs *fsObject) FindParentFsObject(path []string) (*fsObject, error) {
//	parentObject := fs.root
//	if parentObject == nil {
//		// We are root
//		parentObject = fs
//	}
//
//	// Call update on parentObject to populate children if it hasn't happened yet.
//	for i := 0; i < len(path); i++ {
//		if parentObject.childstr == nil {
//			fmt.Printf("Parentobject.childstr is nil\n")
//			return nil, os.ErrNotExist
//		}
//		fmt.Printf("current path: %v, i: %v, len(path): %v\n", path[i], i, len(path))
//		if child, ok := parentObject.childstr[path[i]]; ok {
//			fmt.Printf("Found child: %v\n", child.stat.Name())
//			parentObject = child
//		} else if len(path)-2 == i {
//			fmt.Printf("Found parent: %v\n", parentObject.stat.Name())
//			// This is the parent object
//			return parentObject, nil
//		} else {
//			fmt.Printf("Didn't find parent object\n")
//			// Parent object doesn't exist
//			return nil, os.ErrNotExist
//		}
//	}
//	return nil, os.ErrInvalid
//}
//
//func (fs *fsObject) AddObj(name string, isDir bool) error {
//	fm := os.FileMode(0644)
//	if isDir {
//		fm = os.FileMode(0755)
//	}
//	mdEntry := makeMdEntryNewline('+', Stat{
//		name:    name,
//		size:    0,
//		mode:    fm,
//		modTime: time.Now(),
//		isDir:   isDir,
//		inode:   uuid.New(),
//		sys:     nil,
//	})
//
//	return fs.orfs.mdctx.Append(uuid.UUID(fs.stat.inode).String(), mdEntry)
//}
//
//func (c *Orfs) Mkdir(name string, perm os.FileMode) error {
//	fmt.Fprintf(c.debuglog, "ORFS: Mkdir: %v\n", name)
//	path := pathSplit(name)
//	pObj, err := c.root.FindParentFsObject(path)
//	if err != nil {
//		panic(err)
//	}
//	pObj.AddObj(path[len(path)-1:][0], true)
//
//	return nil
//}

//func (c *Orfs) createFD(name string) *File {
//	return &File{
//		oid:  name,
//		pos:  0,
//		orfs: c,
//	}
//}
//
//func (c *Orfs) OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
//	fmt.Fprintf(c.debuglog, "ORFS: OpenFile: %v\n", name)
//	return c.createFD(name), nil
//}
//
//func (c *Orfs) RemoveAll(name string) error {
//	fmt.Fprintf(c.debuglog, "ORFS: Removeall: %v\n", name)
//	return c.ioctx.Delete(name)
//}
//
//func (c *Orfs) Rename(oldName, newName string) error {
//	fmt.Fprintf(c.debuglog, "ORFS: Rename: %v -> %v", oldName, newName)
//	oldf := c.createFD(oldName)
//	defer oldf.Close()
//	oldfStat, err := oldf.Stat()
//	if err != nil {
//		return err
//	}
//	newf := c.createFD(newName)
//	defer newf.Close()
//	buf := make([]byte, oldfStat.Size()) // create buf of filesize, this sucks but is a quick and dirty fix.
//	read, err := oldf.Read(buf)
//	if err != nil {
//		return err
//	}
//	if int64(read) < oldfStat.Size() {
//		return fmt.Errorf("Failed to read entire file")
//	}
//	write, err := newf.Write(buf)
//	if err != nil {
//		return err
//	}
//	if int64(write) != oldfStat.Size() {
//		return fmt.Errorf("Failed to write entire new file")
//	}
//	return c.RemoveAll(oldName)
//
//	// ceph doesn't support renaming it seems..
//	// We could read the file, write it and then delete the original
//	// but that means we can set us up for quite long-running jobs..
//	//return fmt.Errorf("Renaming not supported")
//}
//
//func (c *Orfs) Stat(name string) (os.FileInfo, error) {
//	fmt.Fprintf(c.debuglog, "orfsDAV: Stat: %v\n", name)
//	f := c.createFD(name)
//	return f.Stat()
//}
