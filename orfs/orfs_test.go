package orfs

import (
	"os"
)

func ExampleNewORFS() {
	datapool := "test"
	metadatapool := "test-metadata"
	cachesize := 1024 * 1024 // number of metadata entries in cache
	fs := NewORFS(datapool, metadatapool, cacheSize)
}

func ExampleOrfs_SetLog() {
	datapool := "test"
	metadatapool := "test-metadata"
	cachesize := 1024 * 1024
	fs := NewORFS(pool, mdpool)
	fs.SetLog(os.Stdout)
}

func ExampleOrfs_SetDebugLog() {
	datapool := "test"
	metadatapool := "test-metadata"
	cachesize := 1024 * 1024
	fs := NewORFS(pool, mdpool, cachesize)
	fs.SetDebugLog(os.Stdout)
}

func ExampleOrfs_Connect() {
	datapool := "test"
	metadatapool := "test-metadata"
	cachesize := 1024 * 1024
	fs := NewORFS(pool, mdpool, cachesize)
	err := fs.Connect()
	if err != nil {
		panic(err)
	}
}

func ExampleOrfs_GetObject() {
	datapool := "test"
	metadatapool := "test-metadata"
	cachesize := 1024 * 1024
	fs := NewORFS(pool, mdpool, cachesize)
	err := fs.Connect()
	if err != nil {
		panic(err)
	}

	// obj is the object for root, "/"
	obj, err := fs.GetObject("/", false)

	// obj is the testfile
	obj, err := fs.GetObject("/testdir/testfile", false)

	// obj is the directory "testdir"
	obj, err := fs.GetObject("/testdir/testfile", true)
}

func ExampleOrfs_Mkdir() {
	fs := NewORFS(pool, mdpool, cachesize)
	err := fs.Connect()
	if err != nil {
		panic(err)
	}
	err := fs.Mkdir("/test", os.FileMode(0755))
	if err != nil {
		return (err)
	}
}

func ExampleOrfs_OpenFile() {
	fs := NewORFS(pool, mdpool, cachesize)
	err := fs.Connect()
	if err != nil {
		panic(err)
	}
	file, err := fs.OpenFile("/test/testfile", 0, os.FileMode(0755))
	if err != nil {
		return (err)
	}
}
