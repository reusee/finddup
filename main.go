package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"

	"github.com/reusee/e"
)

var (
	me     = e.Default.WithStack().WithName("finddup")
	ce, he = e.New(me)
	pt     = fmt.Printf
)

func main() {
	var dir string
	if len(os.Args) > 1 {
		dir = os.Args[1]
	} else {
		dir = "."
	}
	dir, err := filepath.Abs(dir)
	ce(err, "get abs dir: %s", dir)

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
			} else if info.Mode()&os.ModeType > 0 {
				// skip non-regular files
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

	sem := make(chan struct{}, runtime.NumCPU())
	var l sync.Mutex
	byHash1 := make(map[uint64][]int)
	for size, is := range bySize {
		if size == 0 || len(is) == 1 {
			continue
		}
		for _, i := range is {
			i := i
			sem <- struct{}{}
			go func() {
				defer func() {
					<-sem
				}()
				f, err := os.Open(files[i].Path)
				ce(err, "open file: %s", files[i].Path)
				r := &io.LimitedReader{
					R: f,
					N: 64 * 1024 * 1024,
				}
				h := fnv.New64()
				_, err = io.Copy(h, r)
				ce(err, "hash file: %s", files[i].Path)
				f.Close()
				sum := h.Sum64()
				l.Lock()
				byHash1[sum] = append(byHash1[sum], i)
				l.Unlock()
			}()
		}
	}
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	sem = make(chan struct{}, runtime.NumCPU())
	byHash := make(map[uint64][]int)
	for _, is := range byHash1 {
		if len(is) == 1 {
			continue
		}
		for _, i := range is {
			i := i
			sem <- struct{}{}
			go func() {
				defer func() {
					<-sem
				}()
				f, err := os.Open(files[i].Path)
				ce(err, "open file: %s", files[i].Path)
				h := fnv.New64()
				_, err = io.Copy(h, f)
				ce(err, "hash file: %s", files[i].Path)
				f.Close()
				sum := h.Sum64()
				l.Lock()
				byHash[sum] = append(byHash[sum], i)
				l.Unlock()
			}()
		}
	}
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	var iss [][]int
	for _, is := range byHash {
		if len(is) == 1 {
			continue
		}
		is := is
		iss = append(iss, is)
	}

	sort.Slice(iss, func(i, j int) bool {
		return files[iss[i][0]].Info.Size() < files[iss[j][0]].Info.Size()
	})

	for _, is := range iss {
		sort.Slice(is, func(i, j int) bool {
			return files[is[i]].Path < files[is[j]].Path
		})
		for _, i := range is {
			pt("%q\n", files[i].Path)
		}
		pt("\n")
	}
}
