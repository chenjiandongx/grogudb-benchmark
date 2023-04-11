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
	badger "github.com/dgraph-io/badger/v3"
)

type BadgerStorage struct {
	db *badger.DB
}

func (s *BadgerStorage) Put(_ string, k, v []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	})
}

func (s *BadgerStorage) PutIf(_ string, k, v []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			return txn.Set(k, v)
		}
		return nil
	})
}

func (s *BadgerStorage) Get(_ string, k []byte) ([]byte, error) {
	var ret []byte
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}

		err = item.Value(func(val []byte) error {
			ret = val
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	return ret, err
}

func (s *BadgerStorage) Has(_ string, k []byte) (bool, error) {
	var found bool
	err := s.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err == nil {
			found = true
		}
		return nil
	})
	return found, err
}

func (s *BadgerStorage) Del(_ string, k []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

func (s *BadgerStorage) Range(bucket string, fn func(k, v []byte) error) error {
	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(bucket)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				return fn(item.Key(), val)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStorage) Close() error {
	return s.db.Close()
}

const (
	StorageBadger = "badger"
)

func init() {
	Register(StorageBadger, NewBadgerStorage)
}

func NewBadgerStorage(path string) Storage {
	opt := badger.DefaultOptions(path)
	opt.SyncWrites = false
	opt.Logger = nil
	db, err := badger.Open(opt)
	if err != nil {
		panic(err)
	}

	return &BadgerStorage{db: db}
}
