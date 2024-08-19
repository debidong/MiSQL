package database

import (
	"MiSQL/BPlusTree"
	"encoding/binary"
)

const (
	FLNODE        = 3 // type of leaf node
	FLNODE_HEADER = 4 + 8 + 8
	FLNODE_CAP    = (BPlusTree.PAGE_SIZE - FLNODE_HEADER) / 8
)

// Node is the struct for node of freelist
// Structure of a FlNode:
// HEADER(TYPE 2B, SIZE 2B, NUMNODES 8B, NEXT 8B) - POINTERS size*8B

// freelist is like stack

type FreeList struct {
	head uint64 // pointer to the first freelist node

	get func(uint64) BPlusTree.Node
	new func(node BPlusTree.Node) uint64
	use func(uint64, BPlusTree.Node)
}

func (fl *FreeList) NumPage() int {
	node := fl.get(fl.head)
	return int(binary.LittleEndian.Uint64(node[4:]))
}

// Get returns the index-th element at the top of the stack.
func (fl *FreeList) Get(idx int) uint64 {
	node := fl.get(fl.head)
	for idx >= flnSize(node) {
		idx -= flnSize(node)
		node = fl.get(flnNext(node))
	}
	return flnPtr(node, flnSize(node)-idx-1)
}

// Update removes certain amount of pointers of pages from freelist to be used and add certain pointers of freed pages
// to freelist.
func (fl *FreeList) Update(nFreePagesRequired int, pagesFreed []uint64) {
	if nFreePagesRequired == 0 && len(pagesFreed) == 0 {
		return
	}

	nPage := fl.NumPage()
	ptrReuse := []uint64{} // reused pointers to free pages during operation

	for fl.head != 0 && len(ptrReuse)*FLNODE_CAP < len(pagesFreed) { //
		node := fl.get(fl.head)
		pagesFreed = append(pagesFreed, fl.head)

		if nFreePagesRequired >= flnSize(node) {
			nFreePagesRequired -= flnSize(node)
		} else {
			nRemain := flnSize(node) - nFreePagesRequired
			nFreePagesRequired = 0

			for nRemain > 0 && len(ptrReuse)*FLNODE_CAP < len(pagesFreed)+nRemain {
				nRemain--
				ptrReuse = append(ptrReuse, flnPtr(node, nRemain))
			}

			for i := 0; i < nRemain; i++ {
				pagesFreed = append(pagesFreed, flnPtr(node, i))
			}
		}

		nPage -= flnSize(node)
		fl.head = flnNext(node)
	}
	flPush(fl, pagesFreed, ptrReuse)
	flnSetNumNodes(fl.get(fl.head), uint64(nPage+len(pagesFreed)))

}

func flPush(fl *FreeList, ptrFreed []uint64, ptrReuse []uint64) {

}

/* callbacks for freelists */

func (db *DB) pageAppend(node BPlusTree.Node) uint64 {
	ptr := db.page.nFlushed + db.page.nAppend
	db.page.nAppend++
	db.page.updates[ptr] = node
	return ptr
}

func (db *DB) pageUse(ptr uint64, node BPlusTree.Node) {
	db.page.updates[ptr] = node
}

/* end callbacks */

// flnSize returns amount of pointers in a freelist node.
func flnSize(node BPlusTree.Node) int {
	return int(binary.LittleEndian.Uint16(node[2:]))
}

// flnNext returns the pointer of next freelist node.
func flnNext(node BPlusTree.Node) uint64 {
	return binary.LittleEndian.Uint64(node[12:])
}

// flnPtr returns the nth pointer in a freelist node.
func flnPtr(node BPlusTree.Node, idx int) uint64 {
	pos := FLNODE_HEADER + idx*8
	return binary.LittleEndian.Uint64(node[pos:])
}

// flnSetPtr sets the nth pointer in a freelist node.
func flnSetPtr(node BPlusTree.Node, idx int, ptr uint64) {
	pos := FLNODE_HEADER + idx*8
	binary.LittleEndian.PutUint64(node[pos:], ptr)
}

// flnSetHeader sets the header of a freelist node with the size and pointer to next node.
func flnSetHeader(node BPlusTree.Node, size uint16, next uint64) {
	binary.LittleEndian.PutUint64(node[0:], FLNODE)
	binary.LittleEndian.PutUint16(node[2:], size)
	binary.LittleEndian.PutUint64(node[12:], next)
}

// flnSetNumNodes sets number of total items in the freelist.
func flnSetNumNodes(node BPlusTree.Node, numNodes uint64) {
	binary.LittleEndian.Uint64(node[4:])
}
