// Package btree
// pager
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
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const PAGE_SIZE = 1024  // Page size
const HEADER_SIZE = 256 // next (overflowed)

// Pager manages pages in a file
type Pager struct {
	file             *os.File                // file to store pages
	deletedPages     []int64                 // list of deleted pages
	deletedPagesLock *sync.Mutex             // lock for deletedPages
	deletedPagesFile *os.File                // file to store deleted pages
	pageLocks        map[int64]*sync.RWMutex // locks for pages
	pageLocksLock    *sync.RWMutex           // lock for pagesLocks
	StatLock         *sync.RWMutex           // lock for stats
	cachedCount      int64                   // cached count of pages
	lastCacheTime    time.Time               // last time the cache was updated
	interval         time.Duration           // interval to update the cache
}

// OpenPager opens a file for page management
func OpenPager(filename string, flag int, perm os.FileMode) (*Pager, error) {
	file, err := os.OpenFile(filename, flag, perm)
	if err != nil {
		return nil, err
	}

	// open the deleted pages file
	deletedPagesFile, err := os.OpenFile(filename+".del", os.O_CREATE|os.O_RDWR, perm)
	if err != nil {
		return nil, err
	}

	// read the deleted pages
	deletedPages, err := readDelPages(deletedPagesFile)
	if err != nil {
		return nil, err
	}

	pgLocks := make(map[int64]*sync.RWMutex)

	// Read the tree file and create locks for each page
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	for i := int64(0); i < stat.Size()/PAGE_SIZE; i++ {
		pgLocks[i] = &sync.RWMutex{}
	}

	return &Pager{file: file, deletedPages: deletedPages, deletedPagesFile: deletedPagesFile, deletedPagesLock: &sync.Mutex{}, pageLocks: pgLocks, pageLocksLock: &sync.RWMutex{}, StatLock: &sync.RWMutex{}, interval: time.Minute * 10, lastCacheTime: time.Now().Add(time.Minute * 10)}, nil
}

// writeDelPages writes the deleted pages that are in-memory to the deleted pages file
func (p *Pager) writeDelPages() error {

	// Truncate the file
	err := p.deletedPagesFile.Truncate(0)
	if err != nil {
		return err
	}

	// Seek to the start of the file
	_, err = p.deletedPagesFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Write the deleted pages to the file
	_, err = p.deletedPagesFile.WriteAt([]byte(strings.Join(strings.Fields(fmt.Sprint(p.deletedPages)), ",")), 0)
	if err != nil {
		return err
	}

	return nil
}

// readDelPages reads the deleted pages from the deleted pages file
func readDelPages(file *os.File) ([]int64, error) {
	pages := make([]int64, 0)

	// stored in comma separated format
	// i.e. 1,2,3,4,5
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return pages, nil
	}

	data = bytes.TrimLeft(data, "[")
	data = bytes.TrimRight(data, "]")

	// split the data into pages
	pagesStr := strings.Split(string(data), ",")

	for _, pageStr := range pagesStr {
		// convert the string to int64
		page, err := strconv.ParseInt(pageStr, 10, 64)
		if err != nil {
			continue
		}

		pages = append(pages, page)

	}

	return pages, nil
}

// splitDataIntoChunks splits data into chunks of PAGE_SIZE
func splitDataIntoChunks(data []byte) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(data); i += PAGE_SIZE {
		end := i + PAGE_SIZE

		// Check if end is beyond the length of data
		if end > len(data) {
			end = len(data)
		}

		chunks = append(chunks, data[i:end])
	}
	return chunks
}

// WriteTo writes data to a specific page
func (p *Pager) WriteTo(pageID int64, data []byte) error {
	// lock the page
	p.getPageLock(pageID).Lock()
	defer p.getPageLock(pageID).Unlock()

	p.DeletePage(pageID)
	// remove from deleted pages
	p.deletedPagesLock.Lock()
	defer p.deletedPagesLock.Unlock()

	for i, page := range p.deletedPages {
		if page == pageID {
			p.deletedPages = append(p.deletedPages[:i], p.deletedPages[i+1:]...)
		}

	}

	// the reason we are doing this is because we are going to write to the page thus having any overflowed pages which are linked to the page may not be needed

	// check if data is larger than the page size
	if len(data) > PAGE_SIZE {
		// create an array [][]byte
		// each element is a page

		chunks := splitDataIntoChunks(data)

		// clear data to free up memory
		data = nil

		headerBuffer := make([]byte, HEADER_SIZE)

		// We need to create pages for each chunk
		// after index 0
		// the next page is the current page + 1

		// index 0 would have the next page of index 1 index 1 would have the next page of index 2

		for i, chunk := range chunks {
			// check if we are at the last chunk
			if i == len(chunks)-1 {
				headerBuffer = make([]byte, HEADER_SIZE)
				nextPage := pageID + 1
				copy(headerBuffer, strconv.FormatInt(nextPage, 10))

				// if chunk is less than PAGE_SIZE, we need to pad it with null bytes
				if len(chunk) < PAGE_SIZE {
					chunk = append(chunk, make([]byte, PAGE_SIZE-len(chunk))...)
				}

				// write the chunk to the file
				_, err := p.file.WriteAt(append(headerBuffer, chunk...), pageID*(PAGE_SIZE+HEADER_SIZE))
				if err != nil {
					return err
				}

			} else {
				// update the header
				headerBuffer = make([]byte, HEADER_SIZE)
				nextPage := pageID + 1
				copy(headerBuffer, strconv.FormatInt(nextPage, 10))

				if len(chunk) < PAGE_SIZE {
					chunk = append(chunk, make([]byte, PAGE_SIZE-len(chunk))...)
				}

				// write the chunk to the file
				_, err := p.file.WriteAt(append(headerBuffer, chunk...), pageID*(PAGE_SIZE+HEADER_SIZE))
				if err != nil {
					return err
				}

				// update the pageID
				pageID = nextPage

			}
		}

	} else {
		// create a buffer to store the header
		headerBuffer := make([]byte, HEADER_SIZE)

		// set the next page to -1
		copy(headerBuffer, "-1")

		// if data is less than PAGE_SIZE, we need to pad it with null bytes
		if len(data) < PAGE_SIZE {
			data = append(data, make([]byte, PAGE_SIZE-len(data))...)
		}

		// write the data to the file
		_, err := p.file.WriteAt(append(headerBuffer, data...), (PAGE_SIZE+HEADER_SIZE)*pageID)
		if err != nil {
			return err
		}

	}

	return nil
}

// getPageLock gets the lock for a page
func (p *Pager) getPageLock(pageID int64) *sync.RWMutex {
	// Lock the mutex that protects the PageLocks map
	p.pageLocksLock.Lock()
	defer p.pageLocksLock.Unlock()

	// Used for page level locking
	// This is decent for concurrent reads and writes
	if lock, ok := p.pageLocks[pageID]; ok {
		return lock
	} else {
		// Create a new lock
		p.pageLocks[pageID] = &sync.RWMutex{}
		return p.pageLocks[pageID]
	}
}

// Write writes data to the next available page
func (p *Pager) Write(data []byte) (int64, error) {

	// check if there are any deleted pages
	if len(p.deletedPages) > 0 {
		// get the last deleted page
		pageID := p.deletedPages[len(p.deletedPages)-1]
		p.deletedPages = p.deletedPages[:len(p.deletedPages)-1]

		err := p.WriteTo(pageID, data)
		if err != nil {
			return -1, err
		}

		return pageID, nil

	} else {
		// get the current file size
		fileInfo, err := p.file.Stat()
		if err != nil {
			return -1, err
		}

		if fileInfo.Size() == 0 {

			err = p.WriteTo(0, data)
			if err != nil {
				return -1, err
			}

			return 0, nil
		}

		// create a new page
		pageId := fileInfo.Size() / (PAGE_SIZE + HEADER_SIZE)

		err = p.WriteTo(pageId, data)
		if err != nil {
			return -1, err
		}

		return pageId, nil

	}

}

// Close closes the file
func (p *Pager) Close() error {
	p.writeDelPages()
	return p.file.Close()
}

// GetPage gets a page and returns the data
// Will gather all the pages that are linked together
func (p *Pager) GetPage(pageID int64) ([]byte, error) {

	// lock the page
	p.getPageLock(pageID).Lock()
	defer p.getPageLock(pageID).Unlock()

	p.deletedPagesLock.Lock()
	// Check if in deleted pages, if so return nil
	if slices.Contains(p.deletedPages, pageID) {
		p.deletedPagesLock.Unlock()
		return nil, nil
	}
	p.deletedPagesLock.Unlock()

	result := make([]byte, 0)

	// get the page
	dataPHeader := make([]byte, PAGE_SIZE+HEADER_SIZE)

	if pageID == 0 {

		_, err := p.file.ReadAt(dataPHeader, 0)
		if err != nil {
			return nil, err
		}
	} else {

		_, err := p.file.ReadAt(dataPHeader, pageID*(PAGE_SIZE+HEADER_SIZE))
		if err != nil {
			return nil, err
		}
	}

	// get header
	header := dataPHeader[:HEADER_SIZE]
	data := dataPHeader[HEADER_SIZE:]

	// remove the null bytes
	header = bytes.Trim(header, "\x00")
	//data = bytes.Trim(data, "\x00")

	// append the data to the result
	result = append(result, data...)

	// get the next page
	nextPage, err := strconv.ParseInt(string(header), 10, 64)
	if err != nil {
		return nil, err
	}

	if nextPage == -1 {
		return result, nil

	}

	for {

		dataPHeader = make([]byte, PAGE_SIZE+HEADER_SIZE)

		_, err := p.file.ReadAt(dataPHeader, nextPage*(PAGE_SIZE+HEADER_SIZE))
		if err != nil {
			break
		}

		// get header
		header = dataPHeader[:HEADER_SIZE]
		data = dataPHeader[HEADER_SIZE:]

		// remove the null bytes
		header = bytes.Trim(header, "\x00")
		//data = bytes.Trim(data, "\x00")

		// append the data to the result
		result = append(result, data...)

		// get the next page
		nextPage, err = strconv.ParseInt(string(header), 10, 64)
		if err != nil || nextPage == -1 {
			break
		}

	}

	return result, nil
}

// GetDeletedPages returns the list of deleted pages
func (p *Pager) GetDeletedPages() []int64 {
	p.deletedPagesLock.Lock()
	defer p.deletedPagesLock.Unlock()
	return p.deletedPages
}

// DeletePage deletes a page
func (p *Pager) DeletePage(pageID int64) error {
	p.deletedPagesLock.Lock()
	defer p.deletedPagesLock.Unlock()

	// Add the page to the deleted pages
	p.deletedPages = append(p.deletedPages, pageID)

	// write the deleted pages to the file
	err := p.writeDelPages()
	if err != nil {
		return err
	}

	return nil
}

// Analyze forces the pager to analyze the file
func (p *Pager) Analyze() error {
	p.StatLock.Lock()
	defer p.StatLock.Unlock()

	// Initialize a counter for the pages
	var pageCount int64 = 0

	// Get the size of the file
	stat, _ := p.file.Stat()
	fileSize := stat.Size()

	// Initialize a counter for the bytes read
	var bytesRead int64 = 0

	// Read through the file in chunks of PAGE_SIZE + HEADER_SIZE bytes
	for bytesRead < fileSize {
		bytesRead += PAGE_SIZE + HEADER_SIZE
		pageCount++
	}

	p.cachedCount = pageCount

	return nil
}

// Count returns the number of pages
func (p *Pager) Count() int64 {

	if p.cachedCount == 0 {
		p.Analyze()

	}

	if time.Since(p.lastCacheTime) > p.interval {
		time.Now()

		p.StatLock.Lock()
		defer p.StatLock.Unlock()

		// Initialize a counter for the pages
		var pageCount int64 = 0

		// Get the size of the file
		stat, _ := p.file.Stat()
		fileSize := stat.Size()

		// Initialize a counter for the bytes read
		var bytesRead int64 = 0

		// Read through the file in chunks of PAGE_SIZE + HEADER_SIZE bytes
		for bytesRead < fileSize {
			bytesRead += PAGE_SIZE + HEADER_SIZE
			pageCount++
		}

		p.cachedCount = pageCount

		// Subtract the number of deleted pages
		//pageCount -= int64(len(p.GetDeletedPages()))

		return pageCount
	} else {
		return p.cachedCount
	}

}
