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
	"path/filepath"

	"go.etcd.io/bbolt"
)

type BboltStorage struct {
	db *bbolt.DB
}

func (s *BboltStorage) Put(bucket string, k, v []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return bucket.Put(k, v)
	})
}

func (s *BboltStorage) PutIf(bucket string, k, v []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		val := bucket.Get(k)
		// not exist
		if val == nil {
			return bucket.Put(k, v)
		}
		return nil
	})
}

func (s *BboltStorage) Get(bucket string, k []byte) ([]byte, error) {
	var val []byte
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		val = bucket.Get(k)
		return nil
	})
	return val, err
}

func (s *BboltStorage) Has(bucket string, k []byte) (bool, error) {
	var val []byte
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		val = bucket.Get(k)
		return nil
	})

	return val != nil, err
}

func (s *BboltStorage) Del(bucket string, k []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return bucket.Delete(k)
	})
}

func (s *BboltStorage) Range(bucket string, fn func(k, v []byte) error) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return bucket.ForEach(fn)
	})
}

func (s *BboltStorage) Close() error {
	return s.db.Close()
}

const (
	StorageBblot = "bblot"
)

func init() {
	Register(StorageBblot, NewBblotStorage)
}

func NewBblotStorage(path string) Storage {
	opt := bbolt.DefaultOptions
	opt.NoSync = true
	db, err := bbolt.Open(filepath.Join(path, "test.db"), 0600, opt)
	if err != nil {
		panic(err)
	}

	return &BboltStorage{db: db}
}
