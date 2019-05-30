package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"

	"github.com/reusee/e"
)

var (
	me, we, ce, he = e.New(e.WithPackage("finddup"))
	pt             = fmt.Printf
)

func main() {
	var dir string
	if len(os.Args) > 1 {
		dir = os.Args[1]
	} else {
		dir = "."
	}
	dir, err := filepath.Abs(dir)
	ce(err)

	type File struct {
		Path string
		Info os.FileInfo
	}
	var files []*File
	var collectFiles func(dir string)
	collectFiles = func(dir string) {
		f, err := os.Open(dir)
		if err != nil {
			pt("%v\n", err)
			return
		}
		defer f.Close()
		infos, err := f.Readdir(1024)
		for _, info := range infos {
			if info.IsDir() {
				collectFiles(filepath.Join(dir, info.Name()))
			} else {
				files = append(files, &File{
					Path: filepath.Join(dir, info.Name()),
					Info: info,
				})
			}
		}
		if err == io.EOF {
			return
		} else if err != nil {
			pt("%v\n", err)
			return
		}
	}
	collectFiles(dir)

	bySize := make(map[int64][]int)
	for i, file := range files {
		bySize[file.Info.Size()] = append(
			bySize[file.Info.Size()],
			i,
		)
	}

	byHash1 := make(map[uint64][]int)
	for size, is := range bySize {
		if size == 0 || len(is) == 1 {
			continue
		}
		for _, i := range is {
			f, err := os.Open(files[i].Path)
			ce(err)
			r := &io.LimitedReader{
				R: f,
				N: 64 * 1024 * 1024,
			}
			h := fnv.New64()
			_, err = io.Copy(h, r)
			ce(err)
			f.Close()
			sum := h.Sum64()
			byHash1[sum] = append(byHash1[sum], i)
		}
	}

	byHash := make(map[uint64][]int)
	for _, is := range byHash1 {
		if len(is) == 1 {
			continue
		}
		for _, i := range is {
			f, err := os.Open(files[i].Path)
			ce(err)
			h := fnv.New64()
			_, err = io.Copy(h, f)
			ce(err)
			f.Close()
			sum := h.Sum64()
			byHash[sum] = append(byHash[sum], i)
		}
	}

	for _, is := range byHash {
		if len(is) == 1 {
			continue
		}
		for _, i := range is {
			pt("%q\n", files[i].Path)
		}
		pt("\n")
	}

}
