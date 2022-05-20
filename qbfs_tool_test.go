package main

import (
	"fmt"
	"qbfs-cli/core"
	"testing"
)

func TestHttp(t *testing.T) {
	fmt.Println("Ignore")
}

/**
The mount table as follows:
/c1/a	=>	hdfs://cluster-1/system
/c1/a/b	=>	hdfs://cluster-1/log
/c2/b	=>	hdfs://cluster-2/system

QBFS Path will be resolved as follows

qbfs://c1/a/example.txt		=> 	hdfs://cluster-1/system/example.txt
qbfs://c1/a/b/example.txt	=>	hdfs://cluster-1/log/example.txt
qbfs://c2/b/example.txt		=>	hdfs://cluster-2/system/example.txt

*/
func TestFsResolve(t *testing.T) {
	mounts := []core.MountInfo{
		core.MountInfo{
			Path:         "c1/a",
			TargetFsPath: "hdfs://cluster-1/system",
		},
		core.MountInfo{
			Path:         "c1/a/b",
			TargetFsPath: "hdfs://cluster-1/log",
		},
		core.MountInfo{
			Path:         "c2/b",
			TargetFsPath: "hdfs://cluster-2/system",
		},
	}

	testPath1 := "qbfs://c1/a/example.txt"
	result := resolvePath(mounts, testPath1, false)
	if result != "hdfs://cluster-1/system/example.txt" {
		t.Error()
	}

	testPath2 := "qbfs://c1/a/b/example.txt"
	result = resolvePath(mounts, testPath2, false)
	if result != "hdfs://cluster-1/log/example.txt" {
		t.Error()
	}

	testPath3 := "qbfs://c2/b/example.txt"
	result = resolvePath(mounts, testPath3, false)
	if result != "hdfs://cluster-2/system/example.txt" {
		t.Error()
	}

	testPath4 := "qbfs://c4/a.txt"
	result = resolvePath(mounts, testPath4, false)
	if result != "Not found." {
		t.Error()
	}
}
