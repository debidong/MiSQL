package database

import (
	"MiSQL/bptree"
	"errors"
	"fmt"
	"os"
	"syscall"
)

// mmapInit initializes mmap and returns the size, chunks of the mmap.
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, err
	}

	if fi.Size()%bptree.PAGE_SIZE != 0 {
		return 0, nil, fmt.Errorf("mmap: %w", errors.New("page size is not a multiple of page size"))
	}
	mmapSize := 64 << 20
	if mmapSize%bptree.PAGE_SIZE != 0 {
		return 0, nil, fmt.Errorf("mmap: %w", errors.New("mmap size is not a multiple of page size"))
	}

	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(int(fp.Fd()), 0, mmapSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil

}

// mmapExtend extends memory map when necessary.
func mmapExtend(db *DB, numPage int) error {
	if db.mmap.size >= numPage*bptree.PAGE_SIZE {
		return nil
	}

	// double the address space of mmap by appending new chunk with the same size as the existing total chunks
	chunk, err := syscall.Mmap(int(db.fp.Fd()), int64(db.mmap.size), db.mmap.size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.size += db.mmap.size
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}
