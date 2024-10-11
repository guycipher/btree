// Package btree
// BTree implementation tests
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package btree

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	// check for btree.db and btree.db.del files

	_, err = os.Stat("btree.db")
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat("btree.db.del")
	if err != nil {
		t.Fatal(err)
	}

}

func TestBTree_Close(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	err = btree.Close()
	if err != nil {
		t.Fatal(err)
	}

}

func TestBTree_Put(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {

		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

	}

	for i := 0; i < 500; i++ {
		key, err := btree.Get([]byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

		if string(key.V[0]) != strconv.Itoa(i) {
			t.Fatalf("expected value to be %d, got %s", i, key.V[0])
		}

		if key == nil {
			t.Fatal("expected key to be not nil")
		}
	}
}

func TestBTree_Put2(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {

		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

	}

	//btree.PrintTree()

	//for i := 0; i < 500; i++ {
	//	key, err := btree.Get([]byte(strconv.Itoa(i)))
	//	if err != nil {
	//		t.Fatal(err)
	//	}
	//
	//	if key == nil {
	//		t.Fatal("expected key to be not nil")
	//	}
	//}
}

func TestBTree_Delete(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {

		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}

	}

	//btree.PrintTree()

	for i := 0; i < 500; i++ {
		err := btree.Delete([]byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
		key, err := btree.Get([]byte(strconv.Itoa(i)))
		if key != nil {
			t.Fatalf("expected key to be nil")
		}
	}
}

func TestBTree_Range(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%03d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.Range([]byte("010"), []byte("020")) // use padded keys
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 11 {
		t.Fatalf("expected 11 keys, got %d", len(keys))
	}

}

func TestBTree_Remove(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	// put 100 values into a key

	for i := 0; i < 100; i++ {
		err := btree.Put([]byte("key"), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// remove 50 values from the key
	for i := 0; i < 50; i++ {
		err := btree.Remove([]byte("key"), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// get the key
	key, err := btree.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}

	if len(key.V) != 50 {
		t.Fatalf("expected 50 keys, got %d", len(key.V))
	}
}

func TestBTree_NGet(t *testing.T) {
	// NGet gets keys not equal to the key
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	// put 100 values into a key

	for i := 0; i < 100; i++ {
		err := btree.Put([]byte(fmt.Sprintf("key_%d", i)), []byte(strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.NGet([]byte("key_50"))
	if err != nil {
		return
	}

	for _, key := range keys {
		if string(key.K) == "key_50" {
			t.Fatalf("expected key not to be key_50")
		}
	}
}

func TestBTree_InOrderTraversal(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%03d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.InOrderTraversal()
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 500 {
		t.Fatalf("expected 500 keys, got %d", len(keys))
	}

	for i := 0; i < 500; i++ {
		if string(keys[i].K) != fmt.Sprintf("%03d", i) {
			t.Fatalf("expected key to be %03d, got %s", i, keys[i].K)
		}

	}
}

func TestBTree_GreaterThan(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%03d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.GreaterThan([]byte("010"))
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 489 {
		t.Fatalf("expected 489 keys, got %d", len(keys))
	}
}

func TestBTree_NRange(t *testing.T) {

	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%03d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.NRange([]byte("010"), []byte("020")) // use padded keys
	if err != nil {
		t.Fatal(err)
	}

	// Expect 489 keys
	if len(keys) != 489 {
		t.Fatalf("expected 489 keys, got %d", len(keys))

	}
}

func TestBTree_LessThan(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%03d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.LessThan([]byte("010"))
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 10 {
		t.Fatalf("expected 10 keys, got %d", len(keys))
	}

	for i := 0; i < 10; i++ {
		if string(keys[i].K) != fmt.Sprintf("%03d", i) {
			t.Fatalf("expected key to be %03d, got %s", i, keys[i].K)
		}

	}
}

func BenchmarkBTree_Put(b *testing.B) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		b.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < b.N; i++ {
		err := btree.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestBTree_GreaterThanEq(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.GreaterThanEq([]byte(fmt.Sprintf("%d", 5)))
	if err != nil {
		t.Fatal(err)
	}

	expect := []string{
		fmt.Sprintf("%d", 5),
		fmt.Sprintf("%d", 6),
		fmt.Sprintf("%d", 7),
		fmt.Sprintf("%d", 8),
		fmt.Sprintf("%d", 9),
	}

	for i, key := range keys {
		if string(key.K) != expect[i] {
			t.Fatalf("expected key to be %s, got %s", expect[i], key.K)
		}

	}
}

func TestBTree_LessThanEq(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	btree, err := Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
	if err != nil {
		t.Fatal(err)
	}

	defer btree.Close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%d", i) // pad the key with leading zeros
		err := btree.Put([]byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, err := btree.LessThanEq([]byte(fmt.Sprintf("%d", 4)))
	if err != nil {
		t.Fatal(err)
	}

	expect := []string{
		fmt.Sprintf("%d", 0),
		fmt.Sprintf("%d", 1),
		fmt.Sprintf("%d", 2),
		fmt.Sprintf("%d", 3),
	}

	for i, key := range keys {
		if string(key.K) != expect[i] {
			t.Fatalf("expected key to be %s, got %s", expect[i], key.K)
		}
	}
}
