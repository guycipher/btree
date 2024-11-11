// Package btree
// pager tests
// BSD 3-Clause License
//
// Copyright (c) 2024, Alex Gaetano Padula
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its
//     contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package btree

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func TestOpenPager(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")
	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}

	defer pager.Close()

	if pager == nil {
		t.Fatal("expected non-nil pager")
	}

}

func TestPager_Write(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Close()

	pageID, err := pager.Write([]byte("Hello World"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = pager.Write([]byte("Hello World 2"))
	if err != nil {
		t.Fatal(err)
	}

	// Get the page 0
	data, err := pager.GetPage(pageID)
	if err != nil {
		t.Fatal(err)
	}

	if string(bytes.ReplaceAll(data, []byte("\x00"), []byte(""))) != "Hello World" {
		t.Fatalf("expected Hello World, got %s", string(bytes.ReplaceAll(data, []byte("\x00"), []byte(""))))
	}

}

func TestPager_Write2(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Close()

	for i := 0; i < 10000; i++ {
		_, err := pager.Write([]byte(fmt.Sprintf("Hello World %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPager_Count(t *testing.T) {
	defer os.Remove("btree.db")
	defer os.Remove("btree.db.del")

	pager, err := OpenPager("btree.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer pager.Close()

	tt := time.Now()

	for i := 0; i < 1000000; i++ {
		_, err := pager.Write([]byte(fmt.Sprintf("Hello World %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	log.Println("Write 1000000 elements in", time.Since(tt))

	//count := pager.Count()
	//
	//if count != 1000 {
	//	t.Fatalf("expected 1000, got %d", count)
	//}
}
