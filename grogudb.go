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
	"github.com/chenjiandongx/grogudb"
)

type GrogudbStorage struct {
	db *grogudb.DB
}

func (s *GrogudbStorage) Put(bucket string, k, v []byte) error {
	return s.db.GetOrCreateBucket(bucket).Put(k, v)
}

func (s *GrogudbStorage) PutIf(bucket string, k, v []byte) error {
	return s.db.GetOrCreateBucket(bucket).PutIf(k, v)
}

func (s *GrogudbStorage) Get(bucket string, k []byte) ([]byte, error) {
	return s.db.GetOrCreateBucket(bucket).Get(k)
}

func (s *GrogudbStorage) Has(bucket string, k []byte) (bool, error) {
	return s.db.GetOrCreateBucket(bucket).Has(k), nil
}

func (s *GrogudbStorage) Del(bucket string, k []byte) error {
	return s.db.GetOrCreateBucket(bucket).Del(k)
}

func (s *GrogudbStorage) Range(bucket string, fn func(k, v []byte) error) error {
	return s.db.GetOrCreateBucket(bucket).Range(func(key, val grogudb.Bytes) {
		if err := fn(key.B(), val.B()); err != nil {
			panic(err)
		}
	})
}

func (s *GrogudbStorage) Close() error {
	return s.db.Close()
}

const (
	StorageGroguDB = "grogudb"
)

func init() {
	Register(StorageGroguDB, NewGroguDBStorage)
}

func NewGroguDBStorage(path string) Storage {
	db, err := grogudb.Open(path, nil)
	if err != nil {
		panic(err)
	}

	return &GrogudbStorage{db: db}
}
