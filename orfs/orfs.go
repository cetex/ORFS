package orfs

import (
	"fmt"
	"github.com/ceph/go-ceph/rados"
	"io"
	"io/ioutil"
	"os"
)

type Orfs struct {
	conn     *rados.Conn
	ioctx    *rados.IOContext
	mdctx    *rados.IOContext
	pool     string
	mdpool   string
	log      io.Writer
	debuglog io.Writer
}

func NewORFS(pool, mdpool string) *Orfs {
	c := new(Orfs)
	c.pool = pool
	c.mdpool = mdpool
	c.log = ioutil.Discard      // Default to discarding all logs
	c.debuglog = ioutil.Discard // Default to discarding all debug logs
	return c
}

func (c *Orfs) SetLog(log io.Writer) {
	c.log = log
}

func (c *Orfs) SetDebugLog(debugLog io.Writer) {
	c.debuglog = debugLog
}

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
	return nil
}

func (c *Orfs) Mkdir(name string, perm os.FileMode) error {
	fmt.Fprintf(c.debuglog, "ORFS: Mkdir: %v\n", name)
	return nil
}

func (c *Orfs) createFD(name string) *File {
	return &File{
		oid:  name,
		pos:  0,
		orfs: c,
	}
}

func (c *Orfs) OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	fmt.Fprintf(c.debuglog, "ORFS: OpenFile: %v\n", name)
	return c.createFD(name), nil
}

func (c *Orfs) RemoveAll(name string) error {
	fmt.Fprintf(c.debuglog, "ORFS: Removeall: %v\n", name)
	return c.ioctx.Delete(name)
}

func (c *Orfs) Rename(oldName, newName string) error {
	fmt.Fprintf(c.debuglog, "ORFS: Rename: %v -> %v", oldName, newName)
	oldf := c.createFD(oldName)
	defer oldf.Close()
	oldfStat, err := oldf.Stat()
	if err != nil {
		return err
	}
	newf := c.createFD(newName)
	defer newf.Close()
	buf := make([]byte, oldfStat.Size()) // create buf of filesize, this sucks but is a quick and dirty fix.
	read, err := oldf.Read(buf)
	if err != nil {
		return err
	}
	if int64(read) < oldfStat.Size() {
		return fmt.Errorf("Failed to read entire file")
	}
	write, err := newf.Write(buf)
	if err != nil {
		return err
	}
	if int64(write) != oldfStat.Size() {
		return fmt.Errorf("Failed to write entire new file")
	}
	return c.RemoveAll(oldName)

	// ceph doesn't support renaming it seems..
	// We could read the file, write it and then delete the original
	// but that means we can set us up for quite long-running jobs..
	//return fmt.Errorf("Renaming not supported")
}

func (c *Orfs) Stat(name string) (os.FileInfo, error) {
	fmt.Fprintf(c.debuglog, "orfsDAV: Stat: %v\n", name)
	f := c.createFD(name)
	return f.Stat()
}
