package main

import (
	"fmt"
	"github.com/cetex/ORFS/orfs"
	"os"
)

func main() {
	fs := orfs.NewORFS("test", "test_metadata", 100000)
	fs.SetDebugLog(os.Stdout)
	fs.SetLog(os.Stderr)
	if err := fs.Connect(); err != nil {
		panic(err)
	}
	list, err := fs.Root.List()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Read root list, len: %v, data: %+v\n", len(list), list)
	for _, d := range list {
		fmt.Printf("List before add: %+v\n", d)
	}

	if !fs.Root.HasChild("Test") {
		dir, err := orfs.NewObj(fs, "Test", true)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Created new dir, data: %+v\n", dir)
		err = fs.Root.Add(dir)
		if err != nil {
			panic(err)
		}
	} else {
		fmt.Printf("Child \"Test\" already exists under root\n")
	}

	list, err = fs.Root.List()
	if err != nil {
		panic(err)
	}
	for _, d := range list {
		fmt.Printf("Dir after add: %+v\n", d)
	}

	if fs.Root.HasChild("Test") {
		fmt.Printf("Child \"Test\" exists under root\n")
		_, err := fs.Root.Get("Test")
		if err != nil {
			panic(err)
		}
		fmt.Printf("Got child \"Test\"\n")
		//err = fs.Root.Delete(_dir)
		//if err != nil {
		//	panic(err)
		//}
	}
	fmt.Printf("Mkdir /test: %v\n", fs.Mkdir("/test", os.FileMode(0755)))
	fmt.Printf("Removeall /Test: %v\n", fs.RemoveAll("/Test"))
	fmt.Printf("RemoveAll /Test2: %v\n", fs.RemoveAll("/Test2"))
	fmt.Printf("Mkdir /test/subdir: %v\n", fs.Mkdir("/test/subdir", os.FileMode(0755)))
	finfo, err := fs.Stat("/test")
	fmt.Printf("Stat /test: %v: %v\n", err, finfo)
	fmt.Printf("Rename /test to /test99: %v\n", fs.Rename("/test", "/test999"))
}
