// Package btree
// An embedded concurrent, disk based, BTree implementation
// Copyright (C) 2024 Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of  MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
// more details.
//
// You should have received a copy of the GNU General Public License along with
// this program.  If not, see <http://www.gnu.org/licenses/>.
package btree

import (
	"errors"
	"os"
	"sync"
)

const PAGE_SIZE = 1024 // Page size for each node within the tree file

// BTree is the main BTree struct
type BTree struct {
	File      *os.File                // The open btree file
	T         int                     // The order of the tree
	TreeLock  *sync.RWMutex           // Lock for the tree file
	PageLocks map[int64]*sync.RWMutex // Locks for each btree node, consumes some memory but allows for concurrent reads
}

// Key is the key struct for the BTree
type Key struct {
	K            interface{}   // Key can be of uint, int, uint64, float64, string, or []byte
	V            []interface{} // key can have multiple values
	Overflowed   bool          // If the key has overflowed, it will be stored in the overflow page
	OverflowPage int64         // the page where we can find the overflowed values. An overflow node has 1 key, no children and a list of values tied to the key. The overflow node [0] key can also be overflowed
}

// Node is the node struct for the BTree
type Node struct {
	Page     int64   // The page number of the node
	Keys     []*Key  // The keys in node
	Children []int64 // The children of the node
	Leaf     bool    // If the node is a leaf node
	Overflow bool    // If the node is an overflow node
	Reuse    bool    // If node will be reused
}

// Open opens a new or existing BTree
func Open(name string, perm int, t int) (*BTree, error) {
	if t < 2 {
		return nil, errors.New("t must be greater than 1")

	}

	treeFile, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.FileMode(perm))
	if err != nil {
		return nil, err
	}

	pgLocks := make(map[int64]*sync.RWMutex)

	// Read the tree file and create locks for each page
	stat, err := treeFile.Stat()
	if err != nil {
		return nil, err
	}

	for i := int64(0); i < stat.Size()/PAGE_SIZE; i++ {
		pgLocks[i] = &sync.RWMutex{}
	}

	return &BTree{
		File:      treeFile,
		PageLocks: pgLocks,
		T:         t,
		TreeLock:  &sync.RWMutex{},
	}, nil
}

// Close closes the BTree
func (b *BTree) Close() error {
	return b.File.Close()
}

// Put inserts a key into the BTree
// A key can have multiple values
func (b *BTree) Put(key interface{}, value interface{}) error {

	return nil
}

// Delete deletes a key and its values from the BTree
func (b *BTree) Delete(k interface{}) error {

	return nil
}

// DeleteValueFromKey deletes a value from a key
func (b *BTree) DeleteValueFromKey(key interface{}, value interface{}) error {

}

// Get gets the values of a key
func (b *BTree) Get(k interface{}) ([]interface{}, error) {

	return nil, nil
}
