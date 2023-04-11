// Copyright 2023 The grogudb Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Storage interface {
	Put(bucket string, k, v []byte) error
	PutIf(bucket string, k, v []byte) error
	Get(bucket string, k []byte) ([]byte, error)
	Del(bucket string, k []byte) error
	Has(bucket string, k []byte) (bool, error)
	Range(bucket string, fn func(k, v []byte) error) error
	Close() error
}

var registerStorage = map[string]func(path string) Storage{}

func Register(name string, fn func(path string) Storage) {
	registerStorage[name] = fn
}

func bucketNum(i int) []byte {
	return []byte(fmt.Sprintf("bucket%d", i))
}

func keyNum(bucket, i int) []byte {
	return []byte(fmt.Sprintf("bucket%d:key%d", bucket, i))
}

func valNum(bucket, i int) []byte {
	return []byte(fmt.Sprintf("bucket%d:val%d", bucket, i))
}

func makeTmpDir() string {
	dir, err := os.MkdirTemp("", "grogudb-benchmark")
	// fmt.Println("mkdir:", dir)
	if err != nil {
		panic(err)
	}
	return dir
}

func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

const outputFormat = "Storage: %s\tOperation: %s\tIter: %d\tBucket: %d\tElapsed: %v\tOps: %f/s\n"

type Option struct {
	Iter        int
	Buckets     int
	StorageType string
	Operation   string
}

var defOptions = &Option{
	Iter:        100000,
	Buckets:     100,
	StorageType: StorageGroguDB,
	Operation:   OpPutUnique,
}

func benchmarkPut(stor Storage, unique bool) {
	wg := sync.WaitGroup{}
	for i := 0; i < defOptions.Buckets; i++ {
		n := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < defOptions.Iter; j++ {
				bucket := string(bucketNum(n))
				var key []byte
				if unique {
					key = keyNum(n, j)
				} else {
					key = keyNum(n, 0)
				}
				err := stor.Put(bucket, key, valNum(n, j))
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkPutUniqueKey(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	start := time.Now()
	benchmarkPut(stor, true)

	since := time.Since(start)
	ops := float64(defOptions.Buckets*defOptions.Iter) / since.Seconds()
	fmt.Printf(outputFormat, storageType, OpPutUnique, defOptions.Iter, defOptions.Buckets, since, ops)
}

func BenchmarkPutDuplicateKey(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	start := time.Now()
	benchmarkPut(stor, false)

	since := time.Since(start)
	ops := float64(defOptions.Buckets*defOptions.Iter) / since.Seconds()
	fmt.Printf(outputFormat, storageType, OpPutDuplicate, defOptions.Iter, defOptions.Buckets, since, ops)
}

func BenchmarkPutIf(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < defOptions.Buckets; i++ {
		n := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < defOptions.Iter; j++ {
				bucket := string(bucketNum(n))
				err := stor.PutIf(bucket, keyNum(n, 0), valNum(n, j))
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
	since := time.Since(start)
	ops := float64(defOptions.Buckets*defOptions.Iter) / since.Seconds()
	fmt.Printf(outputFormat, storageType, OpPutIf, defOptions.Iter, defOptions.Buckets, since, ops)
}

func BenchmarkRange(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	benchmarkPut(stor, true)
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < defOptions.Buckets; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()
			err := stor.Range(string(bucketNum(n)), func(k, v []byte) error {
				return nil
			})
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	since := time.Since(start)
	ops := float64(1 / since.Seconds())
	fmt.Printf(outputFormat, storageType, OpRange, defOptions.Iter, defOptions.Buckets, since, ops)
}

func BenchmarkHas(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	benchmarkPut(stor, true)
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < defOptions.Buckets; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()

			bucket := string(bucketNum(n))
			for j := 0; j < defOptions.Iter; j++ {
				ok, err := stor.Has(bucket, keyNum(n, j))
				if err != nil || !ok {
					panic(storageType + ":Has")
				}
			}
		}()
	}
	wg.Wait()
	since := time.Since(start)
	ops := float64(defOptions.Buckets*defOptions.Iter) / since.Seconds()
	fmt.Printf(outputFormat, storageType, OpHas, defOptions.Iter, defOptions.Buckets, since, ops)
}

func BenchmarkDel(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	benchmarkPut(stor, true)
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < defOptions.Buckets; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()

			bucket := string(bucketNum(n))
			for j := 0; j < defOptions.Iter; j++ {
				err := stor.Del(bucket, keyNum(n, j))
				if err != nil {
					panic(storageType + ":Del")
				}
			}
		}()
	}
	wg.Wait()
	since := time.Since(start)
	ops := float64(defOptions.Buckets*defOptions.Iter) / since.Seconds()
	fmt.Printf(outputFormat, storageType, OpDel, defOptions.Iter, defOptions.Buckets, since, ops)
}

func BenchmarkGet(storageType string) {
	dir := makeTmpDir()
	defer removeDir(dir)

	stor := registerStorage[storageType](dir)
	defer stor.Close()

	benchmarkPut(stor, true)
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < defOptions.Buckets; i++ {
		wg.Add(1)
		n := i
		go func() {
			defer wg.Done()

			bucket := string(bucketNum(n))
			for j := 0; j < defOptions.Iter; j++ {
				_, err := stor.Get(bucket, keyNum(n, j))
				if err != nil {
					panic(storageType + ":Get")
				}
			}
		}()
	}
	wg.Wait()
	since := time.Since(start)
	ops := float64(defOptions.Buckets*defOptions.Iter) / since.Seconds()
	fmt.Printf(outputFormat, storageType, OpGet, defOptions.Iter, defOptions.Buckets, since, ops)
}

const (
	OpPutUnique    = "PutUnique"
	OpPutDuplicate = "PutDuplicate"
	OpPutIf        = "PutIf"
	OpRange        = "Range"
	OpHas          = "Has"
	OpDel          = "Del"
	OpGet          = "Get"
)

var registerOperation = map[string]func(string){
	OpPutUnique:    BenchmarkPutUniqueKey,
	OpPutDuplicate: BenchmarkPutDuplicateKey,
	OpRange:        BenchmarkRange,
	OpPutIf:        BenchmarkPutIf,
	OpHas:          BenchmarkHas,
	OpDel:          BenchmarkDel,
	OpGet:          BenchmarkGet,
}

func main() {
	flag.IntVar(&defOptions.Iter, "iter", 100000, "iterations")
	flag.IntVar(&defOptions.Buckets, "buckets", 100, "buckets")
	flag.StringVar(&defOptions.StorageType, "storage", StorageGroguDB, "storage")
	flag.StringVar(&defOptions.Operation, "operation", OpPutUnique, "operation")
	flag.Parse()

	_, ok := registerStorage[defOptions.StorageType]
	if !ok {
		var s []string
		for k := range registerStorage {
			s = append(s, k)
		}
		fmt.Printf("no storage found, only [%s] supported\n", strings.Join(s, ","))
	}

	fn, ok := registerOperation[defOptions.Operation]
	if !ok {
		var s []string
		for k := range registerOperation {
			s = append(s, k)
		}
		fmt.Printf("no operation found, only [%s] supported\n", strings.Join(s, ","))
	}

	fn(defOptions.StorageType)
}
