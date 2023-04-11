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
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDBStorage struct {
	db *leveldb.DB
}

func (s *LevelDBStorage) Put(_ string, k, v []byte) error {
	return s.db.Put(k, v, nil)
}

func (s *LevelDBStorage) PutIf(_ string, k, v []byte) error {
	ok, err := s.db.Has(k, nil)
	if err != nil {
		return err
	}
	if !ok {
		return s.db.Put(k, v, nil)
	}
	return nil
}

func (s *LevelDBStorage) Get(_ string, k []byte) ([]byte, error) {
	return s.db.Get(k, nil)
}

func (s *LevelDBStorage) Has(_ string, k []byte) (bool, error) {
	return s.db.Has(k, nil)
}

func (s *LevelDBStorage) Del(_ string, k []byte) error {
	return s.db.Delete(k, nil)
}

func (s *LevelDBStorage) Range(bucket string, fn func(k, v []byte) error) error {
	it := s.db.NewIterator(util.BytesPrefix([]byte(bucket)), nil)
	defer it.Release()

	for it.First(); it.Next(); {
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return nil
}

func (s *LevelDBStorage) Close() error {
	return s.db.Close()
}

const (
	StorageLevelDB = "leveldb"
)

func init() {
	Register(StorageLevelDB, NewLevelDBStorage)
}

func NewLevelDBStorage(path string) Storage {
	opts := &opt.Options{NoSync: true}
	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		panic(err)
	}

	return &LevelDBStorage{db: db}
}
