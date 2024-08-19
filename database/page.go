package database

import (
	"MiSQL/BPlusTree"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"syscall"
)

const DB_SIG = "MiSQLMasterPage"

type Page struct {
	nFlushed uint64            // db size in number of pages
	nFree    int               // number of pages taken from freelist
	nAppend  uint64            // number of temporary pages to be appended
	updates  map[uint64][]byte // pending updates, including appending pages
}

// pageGet obtains a page given with its pointer by checking in memory map. It serves as the callback function for
// BP tree.
func (db *DB) pageGet(ptr uint64) BPlusTree.Node {
	// if this page is temporarily stored and not flushed into disk
	if page, ok := db.page.updates[ptr]; ok {
		return page
	}

	// else this page is in disk
	return pageGetMapped(db, ptr)

}

func pageGetMapped(db *DB, ptr uint64) []byte {
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BPlusTree.PAGE_SIZE
		if ptr < end {
			offset := BPlusTree.PAGE_SIZE * (ptr - start)
			return chunk[offset : offset+BPlusTree.PAGE_SIZE]
		}
		start = end
	}
	return nil
}

/* callbacks for BP tree */
func (db *DB) pageNew(node BPlusTree.Node) uint64 {
	ptr := uint64(0)
	if db.page.nFree < db.fl.NumPage() {
		// there are still page unused in the freelist, then use them instead of appending new pages
		ptr = db.fl.Get(db.page.nFree)
		db.page.nFree++
	} else {
		// append new page
		ptr = db.page.nAppend + db.page.nFlushed
		db.page.nAppend++
	}
	db.page.updates[ptr] = node
	return ptr
}

func (db *DB) pageDel(ptr uint64) {
	db.page.updates[ptr] = nil
}

/* ends callbacks */

func flushPages(db *DB) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

func writePages(db *DB) error {
	// update freelist first
	freed := []uint64{}
	for ptr, page := range db.page.updates {
		if page == nil {
			freed = append(freed, ptr)
		}
	}
	db.fl.Update(db.page.nFree, freed)

	// check if it's necessary to extend file or mmap
	numPage := int(db.page.nFlushed) + len(db.page.updates)
	if err := fileExtend(db, numPage); err != nil {
		return err
	}
	if err := mmapExtend(db, numPage); err != nil {
		return err
	}

	// flush updates to disks
	for ptr, page := range db.page.updates {
		if page != nil {
			copy(pageGetMapped(db, ptr), page)
		}
	}

	return nil
}

func syncPages(db *DB) error {
	// sync written pages
	if err := db.fp.Sync(); err != nil {
		return err
	}

	// discard buffers
	db.page.nFlushed += uint64(len(db.page.updates))
	db.page.updates = make(map[uint64][]byte)

	// update meta page
	if err := metaPageUpdate(db); err != nil {
		return err
	}

	// sync updated meta page
	if err := db.fp.Sync(); err != nil {
		return err
	}

	return nil
}

// Meta page is the first page to store pointers to root pages and other important stuff.
// Structure of meta page:
// Signature(16B), BP tree root pointer(8B), number of flushed pages(8B), freelist head root pointer(8B)

// metaPageLoad checks meta page and updates BP tree root pointers and page amount.
func metaPageLoad(db *DB) error {
	if db.fsize == 0 { // empty db file
		db.page.nFlushed = 1
		return nil
	}

	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	pageUsedNum := binary.LittleEndian.Uint64(data[24:])
	flHead := binary.LittleEndian.Uint64(data[32:])

	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("metaPageLoad: bad signature")
	}

	bad := !(pageUsedNum >= 1 && pageUsedNum < uint64(db.fsize/BPlusTree.PAGE_SIZE))
	if bad {
		return errors.New("metaPageLoad: bad meta")
	}

	db.tree.Root = root
	db.page.nFlushed = pageUsedNum
	db.fl.head = flHead
	return nil
}

// metaPageUpdate gets the pointer of BP tree root node and flushed page amount from the memory,
// and updates them in the meta page.
func metaPageUpdate(db *DB) error {
	data := [32]byte{}
	copy(data[:16], []byte(DB_SIG))

	binary.LittleEndian.PutUint64(data[16:], db.tree.Root)
	binary.LittleEndian.PutUint64(data[24:], db.page.nFlushed)
	binary.LittleEndian.PutUint64(data[32:], db.fl.head)

	_, err := syscall.Pwrite(int(db.fp.Fd()), data[:], 0)
	if err != nil {
		return fmt.Errorf("metaPageUpdate: %w", err)
	}

	return nil
}
