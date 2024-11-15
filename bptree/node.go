package bptree

import "encoding/binary"

const (
	BNODE_INTERNAL = 1 // type of non-leaf node
	BNODE_LEAF     = 2 // type of leaf node
)

// Node is the struct for node of B+Tree.
// Structure of a Node:
// BTNODE_HEADER - POINTERS - OFFSETS - KVs
type Node []byte

// BPlusTree is the struct for B+Tree.
// It uses uint64 for the disk page number.
type BPlusTree struct {
	Root uint64
	// callbacks
	Get func(uint64) Node      // returns pointer to a B+tree node
	New func(node Node) uint64 // allocates a new B+tree node and returns its pointer
	Del func(uint64)           // deallocates a B+tree node
}

// BTNODE_HEADER stores type of the node and the amount of KVs in this node.
// Structure of header(4B):
// nodeType(2B) - numKeys(2B)

func (node Node) getNodeType() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node Node) getNumKeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node Node) setHeader(nodeType uint16, numKeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], nodeType)
	binary.LittleEndian.PutUint16(node[2:4], numKeys)
}

// POINTERS stores uint64 representing the disk page where the node is located.
// The length of the pointers area is (8*numKeys), with each pointers taking 64bits.

func (node Node) getPtr(index uint16) uint64 {
	pos := BTNODE_HEADER + index*8
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node Node) setPtr(index uint16, val uint64) {
	pos := BTNODE_HEADER + index*8
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// OFFSETS stores the relative positions (bytes length) from KVs to the first database, started from the 2nd database.
// The last element in OFFSETS stores the relative position from the end of last database to the first database, which is
// actually the length of the whole node.
//
// The length of offset area is (2B * numKeys), with each elements taking 2B. In that case offset supports each database with
// the maximum length of 64KB.

func (node Node) getOffsetPos(index uint16) uint16 {
	return BTNODE_HEADER + node.getNumKeys()*8 + (index-1)*2
}

func (node Node) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	pos := node.getOffsetPos(index)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node Node) setOffset(index uint16, val uint16) {
	binary.LittleEndian.PutUint16(node[node.getOffsetPos(index):], val)
}

func (node Node) getKVPos(index uint16) uint16 {
	return BTNODE_HEADER + (8+2)*node.getNumKeys() + node.getOffset(index)
}

// Structure of every database pair:
// keyLen(2B) - valLen(2B) - key - val

func (node Node) getKey(index uint16) []byte {
	pos := node.getKVPos(index)
	keyLen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:keyLen]
}

func (node Node) getVal(index uint16) []byte {
	pos := node.getKVPos(index)
	keyLen := binary.LittleEndian.Uint16(node[pos:])
	valLen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+keyLen:][:valLen]
}

// nodeSizeBytes Get the size of the node in bytes.
func (node Node) nodeSizeBytes() uint16 {
	return node.getKVPos(node.getNumKeys())
}
