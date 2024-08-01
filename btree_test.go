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
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
)

// Test open, duh!
// We are testing Close as well no point to write another test for that specifically
func TestOpen(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Error(err)
		return
	}

	if bt == nil {
		t.Error("expected bt to not be nil")
		return
	}

	if bt.File == nil {
		t.Error("expected bt.File to not be nil")
		return
	}

	err = bt.Close()
	if err != nil {
		t.Error(err)
		return
	}

}

func TestEncodeNode(t *testing.T) {
	n := &Node{
		Page: 1,
	}

	_, err := encodeNode(n)
	if err != nil {
		t.Error(err)
		return
	}

	// make a large node exceeding PAGE_SIZE
	n = &Node{
		Page: 1,
		Keys: make([]*Key, 0),
	}

	for i := 0; i < 1000; i++ {
		n.Keys = append(n.Keys, &Key{
			K: i,
			V: []interface{}{i},
		})
	}

	_, err = encodeNode(n)
	if err == nil {
		t.Error("expected error")
		return
	}

	if err.Error() != "node too large to encode" {
		t.Error("unexpected error: " + err.Error())
		return
	}

}

func TestDecodeNode(t *testing.T) {
	n := &Node{
		Page: 1,
	}

	b, err := encodeNode(n)
	if err != nil {
		t.Error(err)
		return
	}

	nn, err := decodeNode(b)
	if err != nil {
		t.Error(err)
		return
	}

	if nn.Page != n.Page {
		t.Error("expected page to be equal")
		return
	}

}

func TestGetPageLock(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Error(err)
		return
	}

	defer bt.Close()

	mu := bt.getPageLock(1)

	if mu == nil {
		t.Error("expected mu to not be nil")
		return
	}

	mu.Lock()
	mu.Unlock()

}

func TestGetPage(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Error(err)
		return
	}

	defer bt.Close()

	// Create new page
	pgN, err := bt.newPageNumber()
	if err != nil {
		t.Error(err)
		return

	}

	// Write page
	n := &Node{
		Page: pgN,
	}

	_, err = bt.writePage(n)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = bt.getPage(0)
	if err != nil {
		t.Error(err)
		return
	}

	// Create new page
	pgN, err = bt.newPageNumber()
	if err != nil {
		t.Error(err)
		return

	}

	// Write page
	n = &Node{
		Page: pgN,
	}

	_, err = bt.writePage(n)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = bt.getPage(1)
	if err != nil {
		t.Error(err)
		return
	}

}

func TestBTree_Put(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Error(err)
		return
	}

	defer bt.Close()

	for i := 0; i < 100; i++ {
		err := bt.Put(i, i)
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func TestBTree_Put2(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Error(err)
		return
	}

	defer bt.Close()

	get, err := bt.Get(1)
	if err != nil {
		t.Error(err)
		return
	}

	log.Println(get)
}

func TestBTree_Delete(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 100; i++ {
		err := bt.Put(i, fmt.Sprintf("value-%d", i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete
	for i := 1; i < 100; i++ {
		err := bt.Delete(i)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBTree_Delete2(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 2; i++ {
		for j := 1; j < 77; j++ {
			err := bt.Put(i, fmt.Sprintf("value-%d", j))
			if err != nil {
				t.Fatal(err)
			}

		}
	}

	// Delete
	for i := 1; i < 2; i++ {
		err := bt.Delete(i)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestBTree_Delete3(t *testing.T) {
	defer os.Remove("test.db")
	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 100; i++ {
		bt.Put(i, fmt.Sprintf("value-%d", i))
	}

	// random from 1 to 100
	n := 1 + rand.Intn(100)

	// Delete
	err = bt.Delete(n)
	if err != nil {
		t.Fatal(err)
	}

	// Get
	values, err := bt.Get(n)
	if err != nil {
		t.Fatal(err)
	}

	if len(values) != 0 {
		t.Fatal("Value not deleted")
	}

}

func TestBTree_Delete4(t *testing.T) {
	// test delete value
	defer os.Remove("test.db")

	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 200; i++ {
		bt.Put(123, fmt.Sprintf("value-%d", i))
	}

	// Delete
	err = bt.Remove(123, "value-121")

	if err != nil {
		t.Fatal(err)
	}

	// Get
	values, err := bt.Get(123)
	if err != nil {
		t.Fatal(err)
	}

	// check for all values and make sure value-121 is not there
	for _, value := range values {
		if value == "value-121" {
			t.Fatal("Value not deleted")
		}
	}

}

func TestIterator(t *testing.T) {
	defer os.Remove("test.db")

	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 20; i++ {
		for j := 1; j < 60; j++ {
			err := bt.Put(i, fmt.Sprintf("value-%d", j))
			if err != nil {
				t.Fatal(err)
			}

		}
	}

	// Iterate
	it, err := bt.NewIteratorFromKey(12)
	if err != nil {
		t.Fatal(err)
	}

	var got []interface{}

	for {
		value, err := it.Next()
		if err != nil {
			break
		}

		got = append(got, value)

	}

	if len(got) != 59 {
		t.Fatal("Expected 59 values")
	}

	for i := 1; i < 60; i++ {
		if got[i-1] != fmt.Sprintf("value-%d", i) {
			t.Fatal("Value mismatch")
		}
	}
}

func TestBTree_Range(t *testing.T) {
	defer os.Remove("test.db")

	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 20; i++ {
		for j := 1; j < 50; j++ {
			err := bt.Put(i, fmt.Sprintf("value-%d", j))
			if err != nil {
				t.Fatal(err)
			}

		}
	}

	// Range
	keys, err := bt.Range(12, 16)
	if err != nil {
		t.Fatal(err)
	}

	expectedKeys := []int{12, 13, 14, 15, 16}

	for i, key := range keys {
		_, ok := key.(*Key)
		if !ok {
			t.Fatal("not of type *Key")
		} else {

			if key.(*Key).K != expectedKeys[i] {
				t.Fatal(err)
			}

			overflow, err := bt.GetKeyOverflow(key.(*Key).K)
			if err != nil {
				t.Fatal(err)
			}

			// overflow should have values 1 to 49
			for j := 1; j < 50; j++ {
				if overflow[j-1] != fmt.Sprintf("value-%d", j) {
					t.Fatal(err)
				}
			}
		}
	}

}

func TestBTree_Get(t *testing.T) {
	defer os.Remove("test.db")

	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 20; i++ {
		for j := 1; j < 50; j++ {
			err := bt.Put(i, fmt.Sprintf("value-%d", j))
			if err != nil {
				t.Fatal(err)
			}

		}
	}

	// Get
	values, err := bt.Get(12)
	if err != nil {
		t.Fatal(err)
	}

	if len(values) != 49 {
		t.Fatal("Expected 49 values")
	}

	for i := 1; i < 50; i++ {
		if values[i-1] != fmt.Sprintf("value-%d", i) {
			t.Fatal("Value mismatch")
		}
	}
}

func TestBTree_Get2(t *testing.T) {
	defer os.Remove("test.db")

	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	// Put
	for i := 1; i < 20; i++ {

		err := bt.Put(i, fmt.Sprintf("value-%d", i))
		if err != nil {
			t.Fatal(err)
		}

	}

	for i := 1; i < 20; i++ {
		values, err := bt.Get(i)
		if err != nil {
			t.Fatal(err)
		}

		if len(values) != 1 {
			t.Fatal("Expected 1 value")
		}

		if values[0] != fmt.Sprintf("value-%d", i) {
			t.Fatal("Value mismatch")
		}
	}
}

func TestPutMultipleValuesManyKeys(t *testing.T) {
	defer os.Remove("test.db")

	bt, err := Open("test.db", 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Close
	defer bt.Close()

	for i := 1; i < 100; i++ {
		for j := 1; j < 100; j++ {
			err := bt.Put(i, fmt.Sprintf("value-%d", j))
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 1; i < 100; i++ {
		log.Println("Getting key", i)
		values, err := bt.Get(i)
		if err != nil {
			t.Fatal(err)
		}

		if len(values) != 99 {
			t.Fatal("Expected 99 values", values)
		}

		for j := 1; j < 100; j++ {
			if values[j-1] != fmt.Sprintf("value-%d", j) {
				t.Fatal("Value mismatch")
			}
			log.Println(values[j-1])
		}
	}

}
