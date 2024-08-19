package database

import (
	"MiSQL/BPlusTree"
	"fmt"
	"os"
	"syscall"
)

/*

Function Call Hierarchy

Set, Del -> updateFileSync -> writePages

*/

type DB struct {
	Path  string
	fp    *os.File
	fsize int
	tree  BPlusTree.BPlusTree

	mmap struct {
		size   int
		chunks [][]byte // pages are stored into chunks, a chunk may contain several pages
	}

	page Page

	fl FreeList
}

// Open (creates and) opens the database file.
func (db *DB) Open() error {
	// create or open db file
	fp, err := createFileSync(db.Path)
	if err != nil {
		return err // no necessary to close db file because of failing to open db file already
	}
	db.fp = fp

	// create mmap
	size, chunk, err := mmapInit(fp)
	if err != nil {
		goto fail
	}
	db.fsize = size
	db.mmap.size = len(chunk)
	db.mmap.chunks = [][]byte{chunk}

	// set callbacks
	db.tree.Get = db.pageGet
	db.tree.New = db.pageNew
	db.tree.Del = db.pageDel

	db.fl.new = db.pageAppend
	db.fl.use = db.pageUse
	db.fl.get = db.pageGet

	// load meta page
	err = metaPageLoad(db)
	if err != nil {
		goto fail
	}

	return nil

fail:
	db.Close()
	return err
}

func (db *DB) Close() error {
	// memory unmap
	for _, chunk := range db.mmap.chunks {
		if err := syscall.Munmap(chunk); err != nil {
			return fmt.Errorf("closing db file: %w", err)
		}
	}
	_ = db.fp.Close()
	return nil
}

func (db *DB) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *DB) Del(key []byte) (bool, error) {
	ok := db.tree.Delete(key)
	return ok, flushPages(db)
}

// deprecated, because we choose not to update root here, but update the meta page when calling syncPages().
// updateFileSync updates database file after modification to B+ tree is done.
//func updateFileSync(db *DB) error {
//	// to ensure the atomicity of an operation, must update nodes first, and update root nodes at last.
//
//	// write new nodes
//	if err := writePages(db); err != nil {
//		return err
//	}
//	// sync
//	if err := syscall.Fsync(int(db.fp.Fd())); err != nil {
//		return err
//	}
//	// update root of B+ tree
//	if err := updateRoot(db); err != nil {
//		return err
//	}
//	// sync
//	return syscall.Fsync(int(db.fp.Fd()))
//}
//
//func updateRoot(db *DB) error {}

func createFileSync(filePath string) (*os.File, error) {
	fp, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	defer func() {
		_ = fp.Close()
	}()

	if err != nil {
		return nil, err
	}

	if err := fp.Sync(); err != nil {
		return nil, err
	}
	return fp, nil
}

func fileExtend(db *DB, pageNum int) error {
	flushedPageNum := db.fsize / BPlusTree.PAGE_SIZE

	if flushedPageNum >= pageNum {
		return nil
	}

	for flushedPageNum < pageNum {
		// exponentially increase file page
		inc := flushedPageNum / 8
		if inc < 1 {
			inc = 1
		}

		flushedPageNum += inc
	}

	fsize := flushedPageNum * BPlusTree.PAGE_SIZE
	// + build darwin
	err := syscall.Ftruncate(int(db.fp.Fd()), int64(fsize))
	// + build linux
	// err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	if err != nil {
		return fmt.Errorf("fileExtend: %w", err)
	}
	return nil

}
