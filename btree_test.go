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

import "testing"

// Test open, duh!
// We are testing Close as well no point to write another test for that specifically
func TestOpen(t *testing.T) {
	bt, err := Open("test.db", 777, 3)
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
