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
	K            interface{}
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

type Iterator struct {
	node     *Node
	keyIndex int
	valIndex int
	btree    *BTree
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

func (b *BTree) NewIteratorFromKey(key interface{}) (*Iterator, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	node, keyIndex, err := b.findNodeForKey(root, key)
	if err != nil {
		return nil, err
	}

	return &Iterator{node: node, keyIndex: keyIndex, valIndex: 0, btree: b}, nil
}

// Close closes the BTree
func (b *BTree) Close() error {
	return b.File.Close()
}

// newBTreeNode
func (b *BTree) newBTreeNode(leaf bool) (*Node, error) {
	newNode := &Node{
		Leaf: leaf,
		Keys: make([]*Key, 0),
	}

	var err error
	newNode.Page, err = b.newPageNumber()
	if err != nil {
		return nil, err
	}

	// We write the node to file
	_, err = b.writePage(newNode)
	if err != nil {
		return nil, err
	}

	// we return the new node
	return newNode, nil
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

// getRoot returns the root of the BTree
func (b *BTree) getRoot() (*Node, error) {
	root, err := b.getPage(0)
	if err != nil {
		if err.Error() == "failed to read page 0: EOF" {
			// create root
			// initial root if a leaf node and starts at page 0
			root = &Node{
				Leaf:     true,
				Page:     0,
				Children: make([]int64, 0),
				Keys:     make([]*Key, 0),
			}

			// write the root to the file
			_, err = b.writePage(root)
			if err != nil {
				return nil, err

			}
		} else {
			return nil, err
		}
	}

	return root, nil
}

// splitRoot splits the root node
func (b *BTree) splitRoot() error {
	oldRoot, err := b.getRoot()
	if err != nil {
		return err
	}

	// Create new node (this will be the new "old root")
	newOldRoot, err := b.newBTreeNode(oldRoot.Leaf)
	if err != nil {
		return err
	}

	// lock the new root
	newRootLock := b.getPageLock(newOldRoot.Page)
	newRootLock.Lock()
	defer newRootLock.Unlock()

	// Copy keys and children from old root to new old root
	newOldRoot.Keys = oldRoot.Keys
	newOldRoot.Children = oldRoot.Children

	// Create new root and make new old root a child of new root
	newRoot := &Node{
		Page:     0, // New root takes the old root's page number
		Children: []int64{newOldRoot.Page},
	}

	// Split new old root and move median key up to new root
	err = b.splitChild(newRoot, 0, newOldRoot)
	if err != nil {
		return err
	}

	// Write new root and new old root to file
	_, err = b.writePage(newRoot)
	if err != nil {
		return err
	}
	_, err = b.writePage(newOldRoot)
	if err != nil {
		return err
	}

	return nil
}

// splitChild splits a child node of x at index i
func (b *BTree) splitChild(x *Node, i int, y *Node) error {
	z, err := b.newBTreeNode(y.Leaf)
	if err != nil {
		return err
	}

	zLock := b.getPageLock(z.Page)
	zLock.Lock()
	defer zLock.Unlock()

	z.Keys = append(z.Keys, y.Keys[b.T:]...)
	y.Keys = y.Keys[:b.T]

	if !y.Leaf {
		z.Children = append(z.Children, y.Children[b.T:]...)
		y.Children = y.Children[:b.T]
	}

	x.Keys = append(x.Keys, nil)
	x.Children = append(x.Children, 0)

	for j := len(x.Keys) - 1; j > i; j-- {
		x.Keys[j] = x.Keys[j-1]
	}
	x.Keys[i] = y.Keys[b.T-1]

	// remove the key from y
	y.Keys = y.Keys[:b.T-1]

	for j := len(x.Children) - 1; j > i+1; j-- {
		x.Children[j] = x.Children[j-1]
	}
	x.Children[i+1] = z.Page

	_, err = b.writePage(y)
	if err != nil {
		return err
	}

	_, err = b.writePage(z)
	if err != nil {
		return err
	}

	_, err = b.writePage(x)
	if err != nil {
		return err
	}

	return nil
}

// Put inserts a key into the BTree
// A key can have multiple values
// Put inserts a key value pair into the BTree
func (b *BTree) Put(key interface{}, value interface{}) error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}

	// lock root
	rootLock := b.getPageLock(root.Page)
	rootLock.Lock()

	// we will unlock the root after we are done
	defer rootLock.Unlock()

	if len(root.Keys) == (2*b.T)-1 {
		err = b.splitRoot()
		if err != nil {
			return err
		}

		root, err = b.getPage(0)
		if err != nil {
			return err

		}
	}

	err = b.insertNonFull(root, key, value)
	if err != nil {
		return err
	}

	return nil

}

// insertNonFull inserts a key into a non-full node
func (b *BTree) insertNonFull(x *Node, key interface{}, value interface{}) error {
	i := len(x.Keys) - 1

	if x.Leaf {
		for i >= 0 && lessThan(key, x.Keys[i].K) {
			i--
		}

		// If key exists, append the value
		if i >= 0 && equal(key, x.Keys[i].K) {

			x.Keys[i].V = append(x.Keys[i].V, value)

			// check if the key has overflowed
			if b.nodeKeyOverflowed(x) {
				// remove last value from the key
				x.Keys[i].V = x.Keys[i].V[:len(x.Keys[i].V)-1]
				err := b.handleKeyOverflow(x, i, key, value)
				if err != nil {
					return err
				}

				return nil

			}
		} else {
			// If key doesn't exist, insert new key and value
			x.Keys = append(x.Keys, nil)
			j := len(x.Keys) - 1
			for j > i+1 {
				x.Keys[j] = x.Keys[j-1]
				j--
			}

			x.Keys[j] = &Key{K: key, V: []interface{}{value}}

		}

		_, err := b.writePage(x)
		if err != nil {
			return err
		}

	} else {
		for i >= 0 && lessThan(key, x.Keys[i].K) {
			i--
		}
		i++
		child, err := b.getPage(x.Children[i])
		if err != nil {
			return err
		}
		if len(child.Keys) == (2*b.T)-1 {
			err = b.splitChild(x, i, child)
			if err != nil {
				return err
			}
			if greaterThan(key, x.Keys[i].K) {
				i++
			}
		}
		child, err = b.getPage(x.Children[i])
		if err != nil {
			return err
		}

		// lock the child
		childLock := b.getPageLock(child.Page)
		childLock.Lock()
		defer childLock.Unlock()

		err = b.insertNonFull(child, key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get returns the values associated with a key
func (b *BTree) Get(k interface{}) ([]interface{}, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.searchKey(k, root)
}

// searchKey searches for a key in the BTree
func (b *BTree) searchKey(k interface{}, x *Node) ([]interface{}, error) {

	if x != nil {

		nodeLock := b.getPageLock(x.Page)
		nodeLock.RLock()
		defer nodeLock.RUnlock()
		i := 0
		for i < len(x.Keys) && lessThan(x.Keys[i].K, k) {
			i++
		}
		if i < len(x.Keys) && equal(k, x.Keys[i].K) {

			var values []interface{}
			values = append(values, x.Keys[i].V...)

			// Check if the key has overflowed
			if x.Keys[i].Overflowed {
				overflowPage, err := b.getPage(x.Keys[i].OverflowPage)
				if err != nil {
					return nil, err
				}

				// Append the values from the overflow page
				values = append(values, overflowPage.Keys[0].V...)

				// Check if the overflow page has overflowed and repeat the process
				for overflowPage.Keys[0].Overflowed {
					overflowPage, err = b.getPage(overflowPage.Keys[0].OverflowPage)
					if err != nil {
						return nil, err
					}
					values = append(values, overflowPage.Keys[0].V...)
				}
			}

			return values, nil
		} else if x.Leaf {
			return nil, nil
		} else {
			child, err := b.getPage(x.Children[i])
			if err != nil {
				return nil, err
			}
			return b.searchKey(k, child)
		}
	} else {
		root, err := b.getRoot()
		if err != nil {
			return nil, err
		}

		return b.searchKey(k, root)
	}
}

// handleKeyOverflow handles the overflow of a key
func (b *BTree) handleKeyOverflow(x *Node, i int, key interface{}, value interface{}) error {
	if x.Keys[i].Overflowed {
		// Get the last overflow page
		overflowPage, err := b.getPage(x.Keys[i].OverflowPage)
		if err != nil {
			return err
		}
		for overflowPage.Keys[0].Overflowed {
			overflowPage, err = b.getPage(overflowPage.Keys[0].OverflowPage)
			if err != nil {
				return err
			}
		}

		// Append the new value to the overflow page
		overflowPage.Keys[0].V = append(overflowPage.Keys[0].V, value)

		// Check if the overflow page has overflowed
		if b.nodeKeyOverflowed(overflowPage) {
			// Remove the last value from the overflow page
			overflowPage.Keys[0].V = overflowPage.Keys[0].V[:len(overflowPage.Keys[0].V)-1]

			// Create a new overflow page
			newOverflowPage, err := b.newBTreeNode(true)
			if err != nil {
				return err
			}

			// Add the value to the new overflow page
			if len(overflowPage.Keys[0].V) == 0 {
				overflowPage.Keys[0].V = append(overflowPage.Keys[0].V, value)
			} else {
				newOverflowPage.Keys = append(newOverflowPage.Keys, &Key{K: key, V: []interface{}{value}})
			}

			// Link the old overflow page to the new one
			overflowPage.Keys[0].Overflowed = true
			overflowPage.Keys[0].OverflowPage = newOverflowPage.Page

			// Write the new overflow page to the file
			_, err = b.writePage(newOverflowPage)
			if err != nil {
				return err
			}
		}

		// Write the old overflow page to the file
		_, err = b.writePage(overflowPage)
		if err != nil {
			return err
		}
	} else {
		existingOverflow, err := b.getAvailableOverflowNode()
		if err != nil {
			return err
		}

		if existingOverflow == nil {
			existingOverflow, err = b.newBTreeNode(true)
			if err != nil {
				return err
			}
		}

		if len(existingOverflow.Keys) == 0 {
			existingOverflow.Keys = append(existingOverflow.Keys, &Key{K: key, V: []interface{}{value}})
		} else {
			existingOverflow.Keys[0].V = append(existingOverflow.Keys[0].V, value)
		}

		x.Keys[i].Overflowed = true
		x.Keys[i].OverflowPage = existingOverflow.Page

		_, err = b.writePage(existingOverflow)
		if err != nil {
			return err
		}

		_, err = b.writePage(x)
		if err != nil {
			return err
		}
	}

	return nil
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

// greaterThan compares two values and returns true if a is greater than b
func greaterThan(a, b interface{}) bool {
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
			return a > b
		}
	case int8:
		if b, ok := b.(int8); ok {
			return a > b
		}
	case int16:
		if b, ok := b.(int16); ok {
			return a > b
		}
	case int32:
		if b, ok := b.(int32); ok {
			return a > b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a > b
		}
	case uint:
		if b, ok := b.(uint); ok {
			return a > b
		}
	case uint8:
		if b, ok := b.(uint8); ok {
			return a > b
		}
	case uint16:
		if b, ok := b.(uint16); ok {
			return a > b
		}
	case uint32:
		if b, ok := b.(uint32); ok {
			return a > b
		}
	case uint64:
		if b, ok := b.(uint64); ok {
			return a > b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a > b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a > b
		}
	case string:
		if b, ok := b.(string); ok {
			return a > b
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return bytes.Compare(a, b) > 0
		}
	}
	return false
}

// equal compares two values and returns true if a is equal than b
func equal(a, b interface{}) bool {
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

			return a == b
		}
	case int8:
		if b, ok := b.(int8); ok {
			return a == b
		}
	case int16:
		if b, ok := b.(int16); ok {
			return a == b
		}
	case int32:
		if b, ok := b.(int32); ok {
			return a == b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a == b
		}
	case uint:
		if b, ok := b.(uint); ok {
			return a == b
		}
	case uint8:
		if b, ok := b.(uint8); ok {
			return a == b
		}
	case uint16:
		if b, ok := b.(uint16); ok {
			return a == b
		}
	case uint32:
		if b, ok := b.(uint32); ok {
			return a == b
		}
	case uint64:
		if b, ok := b.(uint64); ok {
			return a == b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a == b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a == b
		}
	case string:
		if b, ok := b.(string); ok {
			return a == b
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return bytes.Equal(a, b)
		}

	}
	return false
}

// greaterThanEq compares two values and returns true if a greater than or equal to b
func greaterThanEq(a, b interface{}) bool {
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
			return a >= b
		}
	case int8:
		if b, ok := b.(int8); ok {
			return a >= b
		}
	case int16:
		if b, ok := b.(int16); ok {
			return a >= b
		}
	case int32:
		if b, ok := b.(int32); ok {
			return a >= b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a >= b
		}
	case uint:
		if b, ok := b.(uint); ok {
			return a >= b
		}
	case uint8:
		if b, ok := b.(uint8); ok {
			return a >= b
		}
	case uint16:
		if b, ok := b.(uint16); ok {
			return a >= b
		}
	case uint32:
		if b, ok := b.(uint32); ok {
			return a >= b
		}
	case uint64:
		if b, ok := b.(uint64); ok {
			return a >= b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a >= b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a >= b
		}
	case string:
		if b, ok := b.(string); ok {
			return a >= b
		}
	}
	return false
}

// lessThanEq compares two values and returns true if a is less than or equal to b
func lessThanEq(a, b interface{}) bool {
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
			return a <= b
		}
	case int8:
		if b, ok := b.(int8); ok {
			return a <= b
		}
	case int16:
		if b, ok := b.(int16); ok {
			return a <= b
		}
	case int32:
		if b, ok := b.(int32); ok {
			return a <= b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a <= b
		}
	case uint:
		if b, ok := b.(uint); ok {
			return a <= b
		}
	case uint8:
		if b, ok := b.(uint8); ok {
			return a <= b
		}
	case uint16:
		if b, ok := b.(uint16); ok {
			return a <= b
		}
	case uint32:
		if b, ok := b.(uint32); ok {
			return a <= b
		}
	case uint64:
		if b, ok := b.(uint64); ok {
			return a <= b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a <= b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a <= b
		}
	case string:
		if b, ok := b.(string); ok {
			return a <= b
		}
	}
	return false
}

// notEq compares two values and returns true if a is not equal to b
func notEq(a, b interface{}) bool {
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
			return a != b
		}
	case int8:
		if b, ok := b.(int8); ok {
			return a != b
		}
	case int16:
		if b, ok := b.(int16); ok {
			return a != b
		}
	case int32:
		if b, ok := b.(int32); ok {
			return a != b
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a != b
		}
	case uint:
		if b, ok := b.(uint); ok {
			return a != b
		}
	case uint8:
		if b, ok := b.(uint8); ok {
			return a != b
		}
	case uint16:
		if b, ok := b.(uint16); ok {
			return a != b
		}
	case uint32:
		if b, ok := b.(uint32); ok {
			return a != b
		}
	case uint64:
		if b, ok := b.(uint64); ok {
			return a != b
		}
	case float32:
		if b, ok := b.(float32); ok {
			return a != b
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a != b
		}
	case string:
		if b, ok := b.(string); ok {
			return a != b
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return !bytes.Equal(a, b)
		}
	}
	return false
}

// PrintTree prints the tree (for debugging purposes ****)
func (b *BTree) PrintTree() error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}
	err = b.printTree(root, 0)
	if err != nil {
		return err
	}
	return nil
}

// printTree prints the tree (for debugging purposes ****)
func (b *BTree) printTree(x *Node, level int) error {
	if x != nil {

		fmt.Printf("Level %d: ", level)
		for _, key := range x.Keys {
			fmt.Printf("%v ", key.K)
		}
		fmt.Println()

		for _, childPage := range x.Children {
			child, err := b.getPage(childPage)
			if err != nil {
				return err
			}
			err = b.printTree(child, level+1)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// nodeKeyOverflowed checks if a node's keys have overflowed
func (b *BTree) nodeKeyOverflowed(n *Node) bool {
	buff := bytes.NewBuffer([]byte{})

	enc := gob.NewEncoder(buff)
	err := enc.Encode(n)
	if err != nil {
		return false
	}

	if len(buff.Bytes()) > (PAGE_SIZE / 2) {
		return true
	} else {
		return false

	}

}

// getAvailableOverflowNode returns a reusable Node from the BTree file
func (b *BTree) getAvailableOverflowNode() (*Node, error) {
	// Get the total number of pages in the BTree file
	pageCount := b.pageCount()

	// Loop through all the pages
	for i := int64(0); i < pageCount; i++ {
		// Get the Node at the current page
		node, err := b.getPage(i)
		if err != nil {
			return nil, err
		}

		// Check if the Node is marked for reuse
		if node.Reuse {

			// Write the cleared Node back to the file
			_, err = b.writePage(node)
			if err != nil {
				return nil, err
			}

			node.Reuse = false

			// Return the cleared Node
			return node, nil
		}
	}

	// If no reusable Node is found, return nil
	return nil, nil
}

// pageCount returns the total number of pages in the BTree file
func (b *BTree) pageCount() int64 {
	stat, _ := b.File.Stat()
	return stat.Size() / PAGE_SIZE
}

// Delete deletes a key from the BTree
func (b *BTree) Delete(k interface{}) error {
	// when we delete we remove all page locks and recreate after delete

	// lock tree
	b.TreeLock.Lock()
	defer b.TreeLock.Unlock()

	// remove all page locks
	b.PageLocks = make(map[int64]*sync.RWMutex)

	root, err := b.getRoot()
	if err != nil {
		return err
	}

	_, err = b.deleteKey(k, root)
	if err != nil {
		return err
	}

	// recreate page locks
	stat, err := b.File.Stat()
	if err != nil {
		return err
	}

	for i := int64(0); i < stat.Size(); i += PAGE_SIZE {
		b.getPageLock(i)

	}

	return nil
}

// deleteKey deletes a key from the BTree
func (b *BTree) deleteKey(k interface{}, x *Node) (*Node, error) {
	i := 0
	for i < len(x.Keys) && lessThan(x.Keys[i].K, k) {
		i++
	}

	if i < len(x.Keys) && equal(k, x.Keys[i].K) {

		overflows, err := b.getAllOverflowPages(x.Keys[i])
		if err != nil {
			return nil, err
		}

		for _, overflow := range overflows {
			// make the overflow page reusable
			overflowPage, err := b.getPage(overflow)
			if err != nil {
				return nil, err
			}

			overflowPage.Reuse = true
			_, err = b.writePage(overflowPage)
			if err != nil {
				return nil, err
			}
		}

		if x.Leaf {

			x.Keys = append(x.Keys[:i], x.Keys[i+1:]...)
			_, err := b.writePage(x)
			if err != nil {
				return nil, err
			}
		} else {
			y, err := b.getPage(x.Children[i])
			if err != nil {
				return nil, err
			}
			z, err := b.getPage(x.Children[i+1])
			if err != nil {
				return nil, err
			}
			if len(y.Keys) >= b.T {
				predKey, err := b.getPredecessor(y)
				if err != nil {
					return nil, err
				}
				x.Keys[i] = predKey
				_, err = b.deleteKey(predKey, y)
				if err != nil {
					return nil, err
				}
			} else if len(z.Keys) >= b.T {
				succKey, err := b.getSuccessor(z)
				if err != nil {
					return nil, err
				}
				x.Keys[i] = succKey
				_, err = b.deleteKey(succKey, z)
				if err != nil {
					return nil, err
				}
			} else {
				y.Keys = append(y.Keys, x.Keys[i])
				y.Keys = append(y.Keys, z.Keys...)
				y.Children = append(y.Children, z.Children...)
				x.Keys = append(x.Keys[:i], x.Keys[i+1:]...)
				x.Children = append(x.Children[:i+1], x.Children[i+2:]...)
				err = b.deletePageAndUpdate(z.Page, PAGE_SIZE)
				if err != nil {
					return nil, err
				}
				_, err = b.deleteKey(k, y)
				if err != nil {
					return nil, err
				}
			}
			_, err = b.writePage(x)
			if err != nil {
				return nil, err
			}
		}
	} else if !x.Leaf {
		child, err := b.getPage(x.Children[i])
		if err != nil {
			return nil, err
		}
		_, err = b.deleteKey(k, child)
		if err != nil {
			return nil, err
		}
	}

	return x, nil
}

// deletePage deletes a page from the file
func (b *BTree) deletePage(offset int64, length int64) error {
	// Determine the size of chunks to read and write
	chunkSize := int64(PAGE_SIZE)

	// Calculate the new size of the file
	stat, err := b.File.Stat()
	if err != nil {
		return err
	}
	newSize := stat.Size() - length

	// Loop over the file
	for i := offset + length; i < newSize; {
		// Calculate the size of the current chunk
		currentChunkSize := chunkSize
		if i+chunkSize > newSize {
			currentChunkSize = newSize - i
		}

		// Read the chunk
		chunk := make([]byte, currentChunkSize)
		_, err = b.File.ReadAt(chunk, i)
		if err != nil {
			return err
		}

		// Write the chunk
		_, err = b.File.WriteAt(chunk, i-length)
		if err != nil {
			return err
		}

		// Move to the next chunk
		i += currentChunkSize
	}

	// Truncate the file to the new size
	err = b.File.Truncate(newSize)
	if err != nil {
		return err
	}

	// remove page from page locks map
	delete(b.PageLocks, offset/PAGE_SIZE)

	return nil
}

// updatePageNumbers updates the page numbers of all nodes that come after the deleted page
func (b *BTree) updatePageNumbers(x *Node, deletedPage int64) error {
	// If the node's page number is greater than the deleted page's number, decrement it
	if x.Page > deletedPage {
		x.Page--
		_, err := b.writePage(x)
		if err != nil {
			return err
		}
	}

	// Recursively update the children
	for _, childPage := range x.Children {
		child, err := b.getPage(childPage)
		if err != nil {
			return err
		}
		err = b.updatePageNumbers(child, deletedPage)
		if err != nil {
			return err
		}
	}

	return nil
}

// deletePageAndUpdate deletes a page and updates the page numbers of all nodes that come after the deleted page
func (b *BTree) deletePageAndUpdate(offset int64, length int64) error {
	// Delete the page
	err := b.deletePage(offset, length)
	if err != nil {
		return err
	}

	// Get the root of the BTree
	root, err := b.getRoot()
	if err != nil {
		return err
	}

	// Update the page numbers of all nodes that come after the deleted page
	err = b.updatePageNumbers(root, offset)
	if err != nil {
		return err
	}

	return nil
}

// getAllOverflowPages returns all overflow pages for a key
func (b *BTree) getAllOverflowPages(key *Key) ([]int64, error) {
	var overflowPages []int64

	// Check if the key has overflowed
	if key.Overflowed {
		overflowPages = append(overflowPages, key.OverflowPage)

		// Read the overflow page
		overflowPage, err := b.getPage(key.OverflowPage)
		if err != nil {
			return nil, err
		}

		// Check if the key in the overflow page has overflowed
		if overflowPage.Keys[0].Overflowed {
			additionalOverflowPages, err := b.getAllOverflowPages(overflowPage.Keys[0])
			if err != nil {
				return nil, err
			}
			overflowPages = append(overflowPages, additionalOverflowPages...)
		}
	}

	return overflowPages, nil
}

// getPredecessor gets the maximum key in the sub-tree rooted at x
func (b *BTree) getPredecessor(x *Node) (*Key, error) {
	// If x is a leaf node, the maximum key is the last key in x.Keys
	if x.Leaf {
		return x.Keys[len(x.Keys)-1], nil
	}

	// If x is an internal node, the maximum key is the maximum key in the last child of x
	child, err := b.getPage(x.Children[len(x.Children)-1])
	if err != nil {
		return nil, err
	}
	return b.getPredecessor(child)
}

// getSuccessor gets the minimum key in the sub-tree rooted at x
func (b *BTree) getSuccessor(x *Node) (*Key, error) {
	// If x is a leaf node, the minimum key is the first key in x.Keys
	if x.Leaf {
		return x.Keys[0], nil
	}

	// If x is an internal node, the minimum key is the minimum key in the first child of x
	child, err := b.getPage(x.Children[0])
	if err != nil {
		return nil, err
	}
	return b.getSuccessor(child)
}

func (b *BTree) GetKeyOverflow(key interface{}) ([]interface{}, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.searchKeyOverflow(key, root)
}

func (b *BTree) searchKeyOverflow(k interface{}, x *Node) ([]interface{}, error) {
	if x != nil {
		nodeLock := b.getPageLock(x.Page)
		nodeLock.RLock()
		defer nodeLock.RUnlock()

		i := 0
		for i < len(x.Keys) && lessThan(x.Keys[i].K, k) {
			i++
		}
		if i < len(x.Keys) && equal(k, x.Keys[i].K) {
			var values []interface{}
			values = append(values, x.Keys[i].V...)

			if x.Keys[i].Overflowed {
				overflowPage, err := b.getPage(x.Keys[i].OverflowPage)
				if err != nil {
					return nil, err
				}

				values = append(values, overflowPage.Keys[0].V...)

				for overflowPage.Keys[0].Overflowed {
					overflowPage, err = b.getPage(overflowPage.Keys[0].OverflowPage)
					if err != nil {
						return nil, err
					}
					values = append(values, overflowPage.Keys[0].V...)
				}
			}

			return values, nil
		} else if x.Leaf {
			return nil, nil
		} else {
			child, err := b.getPage(x.Children[i])
			if err != nil {
				return nil, err
			}
			return b.searchKeyOverflow(k, child)
		}
	} else {
		root, err := b.getRoot()
		if err != nil {
			return nil, err
		}

		return b.searchKeyOverflow(k, root)
	}
}

func (b *BTree) findNodeForKey(x *Node, key interface{}) (*Node, int, error) {
	i := 0
	for i < len(x.Keys) && lessThan(x.Keys[i].K, key) {
		i++
	}

	if i < len(x.Keys) && equal(key, x.Keys[i].K) {
		return x, i, nil
	} else if !x.Leaf {
		child, err := b.getPage(x.Children[i])
		if err != nil {
			return nil, 0, err
		}

		return b.findNodeForKey(child, key)
	}

	return nil, 0, errors.New("key not found")
}

func (it *Iterator) Next() (interface{}, error) {
	if it.node == nil {
		return nil, errors.New("iterator has no more items")
	}

	// If the iterator has reached the end of the values for a key
	if it.valIndex >= len(it.node.Keys[it.keyIndex].V) {
		// If the key has overflowed, retrieve the overflow page and continue iterating over the values
		if it.node.Keys[it.keyIndex].Overflowed {
			overflowNode, err := it.btree.getPage(it.node.Keys[it.keyIndex].OverflowPage)
			if err != nil {
				return nil, err
			}
			it.node = overflowNode
			it.keyIndex = 0
			it.valIndex = 0
		} else {
			// If the key has not overflowed, move to the next key
			it.keyIndex++
			it.valIndex = 0
		}
	}

	// If the iterator has reached the end of the keys for a node
	if it.keyIndex >= len(it.node.Keys) {
		if it.node.Leaf {
			it.node = nil
			return nil, errors.New("iterator has no more items")
		}
		var err error
		it.node, err = it.btree.getPage(it.node.Children[it.keyIndex])
		if err != nil {
			return nil, err
		}
		it.keyIndex = 0
	}

	//key := it.node.Keys[it.keyIndex].K
	value := it.node.Keys[it.keyIndex].V[it.valIndex]
	it.valIndex++

	return value, nil
}

// Range returns all keys in the BTree that are within the range [start, end]
func (b *BTree) Range(start, end interface{}) ([]interface{}, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.rangeKeys(start, end, root)
}

// rangeKeys returns all keys in the BTree that are within the range [start, end]
func (b *BTree) rangeKeys(start, end interface{}, x *Node) ([]interface{}, error) {
	keys := make([]interface{}, 0)
	if x != nil {
		nodeLock := b.getPageLock(x.Page)
		nodeLock.RLock()
		defer nodeLock.RUnlock()

		i := 0
		for i < len(x.Keys) && lessThan(x.Keys[i].K, start) {
			i++
		}
		for i < len(x.Keys) && lessThanEq(x.Keys[i].K, end) {
			if !x.Leaf {
				child, err := b.getPage(x.Children[i])
				if err != nil {
					return nil, err
				}
				childKeys, err := b.rangeKeys(start, end, child)
				if err != nil {
					return nil, err
				}
				keys = append(keys, childKeys...)
			}
			keys = append(keys, x.Keys[i])
			i++
		}
		if !x.Leaf && i < len(x.Children) {
			child, err := b.getPage(x.Children[i])
			if err != nil {
				return nil, err
			}
			childKeys, err := b.rangeKeys(start, end, child)
			if err != nil {
				return nil, err
			}
			keys = append(keys, childKeys...)
		}
	}
	return keys, nil
}

// deleteValueFromNode deletes a value from a node
func (b *BTree) deleteValueFromNode(x *Node, key interface{}, value interface{}) error {
	for _, k := range x.Keys {
		if equal(k.K, key) {
			// If the key has overflowed, retrieve all overflow pages and delete the value from there
			if k.Overflowed {
				overflowNode, err := b.getPage(k.OverflowPage)
				if err != nil {
					return err
				}
				for {
					for j, v := range overflowNode.Keys[0].V {
						if equal(v, value) {
							overflowNode.Keys[0].V = append(overflowNode.Keys[0].V[:j], overflowNode.Keys[0].V[j+1:]...)
							_, err := b.writePage(overflowNode)
							if err != nil {
								return err
							}
							return nil
						}
					}
					// If the overflow node itself has overflowed, retrieve the next overflow page
					if overflowNode.Keys[0].Overflowed {
						overflowNode, err = b.getPage(overflowNode.Keys[0].OverflowPage)
						if err != nil {
							return err
						}
					} else {
						break
					}
				}
			} else {
				// If the key has not overflowed, delete the value from the key's values
				for j, v := range k.V {
					if equal(v, value) {
						k.V = append(k.V[:j], k.V[j+1:]...)
						return nil
					}
				}
			}
		}
	}

	// If the key was not found in this node and the node is not a leaf, search the children
	if !x.Leaf {
		for _, childPage := range x.Children {
			child, err := b.getPage(childPage)
			if err != nil {
				return err
			}

			err = b.deleteValueFromNode(child, key, value)
			if err != nil {
				return err
			}
		}
	}

	return errors.New("value not found")
}

// Remove removes a keys value from the BTree
func (b *BTree) Remove(key interface{}, value interface{}) error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}

	// lock root
	rootLock := b.getPageLock(root.Page)
	rootLock.Lock()

	// we will unlock the root after we are done
	defer rootLock.Unlock()

	err = b.deleteValueFromNode(root, key, value)
	if err != nil {
		return err
	}

	return nil
}
