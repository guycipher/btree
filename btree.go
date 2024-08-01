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
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
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

	return nil
}

// Get gets the values of a key
func (b *BTree) Get(k interface{}) ([]interface{}, error) {

	return nil, nil
}

// encodeNode encodes a node into a byte slice
// The byte slice is padded with zeros to PAGE_SIZE
func encodeNode(n *Node) ([]byte, error) {
	buff := bytes.NewBuffer([]byte{}) // Create a new buffer to store the encoded node

	enc := gob.NewEncoder(buff) // Create a new encoder passing the buffer
	err := enc.Encode(n)        // Encode the node
	if err != nil {
		return nil, err
	}

	// Check if the node is too large to encode
	if len(buff.Bytes()) > PAGE_SIZE {
		// **** This can occur if you set a T too large with a small PAGE_SIZE
		return nil, errors.New("node too large to encode")
	}

	// Fill the rest of the page with zeros
	for i := len(buff.Bytes()); i < PAGE_SIZE; i++ {
		buff.WriteByte(0)
	}

	return buff.Bytes(), nil

}

// decodeNode decodes a byte slice into a node
func decodeNode(data []byte) (*Node, error) {
	n := &Node{} // Create a new node which will be decoded into

	dec := gob.NewDecoder(bytes.NewBuffer(data)) // Create a new decoder passing the data

	err := dec.Decode(n) // Decode the data into the node
	if err != nil {
		return nil, err

	}

	return n, nil
}

// getPageLock returns the lock for a page
// If the lock does not exist, it creates a new lock
func (b *BTree) getPageLock(pageno int64) *sync.RWMutex {
	// Used for page level locking
	// This is decent for concurrent reads and writes

	if lock, ok := b.PageLocks[pageno]; ok {
		return lock
	} else {
		// Create a new lock
		b.PageLocks[pageno] = &sync.RWMutex{}

		return b.PageLocks[pageno]
	}

	return nil
}

// Get page from file.
// Decodes and returns node
func (b *BTree) getPage(pageno int64) (*Node, error) {

	// Read the page from the file
	page := make([]byte, PAGE_SIZE)
	if pageno == 0 { // if pageno is 0, read from the start of the file

		_, err := b.File.ReadAt(page, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", pageno, err)
		}
	} else {
		_, err := b.File.ReadAt(page, pageno*PAGE_SIZE)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", pageno, err)
		}
	}

	// Unmarshal the page into a node
	node, err := decodeNode(page)
	if err != nil {
		return nil, fmt.Errorf("failed to decode page %d: %w", pageno, err)
	}

	return node, nil
}

// newPageNumber returns the next page number
// to be used before writing a new page
func (b *BTree) newPageNumber() (int64, error) {
	fileInfo, err := b.File.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size() / PAGE_SIZE, nil
}

// writePage encodes a node and writes it to the tree file at node page
func (b *BTree) writePage(n *Node) (int64, error) {

	buff, err := encodeNode(n)
	if err != nil {
		return 0, err
	}

	if n.Page == 0 {
		_, err = b.File.WriteAt(buff, io.SeekStart)
		if err != nil {
			return 0, err
		}
	} else {

		_, err = b.File.WriteAt(buff, n.Page*PAGE_SIZE)
		if err != nil {
			return 0, err
		}
	}
	return n.Page, nil

}

// lessThan compares two values and returns true if a is less than b
func lessThan(a, b interface{}) bool {
	// check if a and b are the same type
	// if not, return false

	aT := fmt.Sprintf("%T", a)
	bT := fmt.Sprintf("%T", b)

	if aT != bT {
		return false
	}

	switch a := a.(type) {
	case int:
		if b, ok := b.(int); ok {
			return a < b
		}
	case int8:
		if b, ok := b.(int8); ok {
			return a < b
		}
	case int16:
		if b, ok := b.(int16); ok {
			return a < b
		}
	case int32:
		if b, ok := b.(int32); ok {
			return a < b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a < b
		}
	case uint:
		if b, ok := b.(uint); ok {
			return a < b
		}
	case uint8:
		if b, ok := b.(uint8); ok {
			return a < b
		}
	case uint16:
		if b, ok := b.(uint16); ok {
			return a < b
		}
	case uint32:
		if b, ok := b.(uint32); ok {
			return a < b
		}
	case uint64:
		if b, ok := b.(uint64); ok {
			return a < b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a < b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a < b
		}
	case string:
		if b, ok := b.(string); ok {
			return a < b
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return bytes.Compare(a, b) < 0

		}
	}
	return false
}
