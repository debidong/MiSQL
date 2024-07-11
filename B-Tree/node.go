package B_Tree

import "encoding/binary"

const (
	BNODE_INTERNAL = 1 // type of non-leaf node
	BNODE_LEAF     = 2 // type of leaf node
)

// BNode is the struct for node of B+Tree.
// Structure of a BNode:
// HEADER - POINTERS - OFFSETS - KVs
type BNode struct {
	data []byte
}

// BTree is the struct for B+Tree.
// It uses uint64 for the disk page number.
type BTree struct {
	root uint64
	get  func(uint64) BNode
	new  func(node BNode) uint64
	del  func(uint64)
}

// HEADER stores type of the node and the amount of KVs in this node.
// Structure of header(4B):
// nodeType(2B) - numKeys(2B)

func (node BNode) getNodeType() uint16 {
	return binary.LittleEndian.Uint16(node.data[0:2])
}

func (node BNode) getNumKeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(nodeType uint16, numKeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], nodeType)
	binary.LittleEndian.PutUint16(node.data[2:4], numKeys)
}

// POINTERS stores uint64 representing the number of disk page where the node is located.
// The length of the pointers area is (8*numKeys), with each pointers taking 64bits.

func (node BNode) getPtr(index uint16) uint64 {
	pos := HEADER + index*8
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(index uint16, val uint64) {
	pos := HEADER + index*8
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// OFFSETS stores the relative positions (bytes length) from KVs to the first KV, started from the 2nd KV.
// The last element in OFFSETS stores the relative position from the end of last KV to the first KV, which is
// actually the length of the whole node.
//
// The length of offset area is (2B * numKeys), with each elements taking 2B. In that case offset supports each KV with
// the maximum length of 64KB.

func (node BNode) getOffsetPos(index uint16) uint16 {
	return HEADER + node.getNumKeys()*8 + (index-1)*2
}

func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	pos := node.getOffsetPos(index)
	return binary.LittleEndian.Uint16(node.data[pos:])
}

func (node BNode) setOffset(index uint16, val uint16) {
	binary.LittleEndian.PutUint16(node.data[node.getOffsetPos(index):], val)
}

func (node BNode) getKVPos(index uint16) uint16 {
	return HEADER + (8+2)*node.getNumKeys() + node.getOffset(index)
}

// Structure of every KV pair:
// keyLen(2B) - valLen(2B) - key - val

func (node BNode) getKey(index uint16) []byte {
	pos := node.getKVPos(index)
	keyLen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:keyLen]
}

func (node BNode) getVal(index uint16) []byte {
	pos := node.getKVPos(index)
	keyLen := binary.LittleEndian.Uint16(node.data[pos:])
	valLen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+keyLen:][:valLen]
}

// nodeSizeBytes Get the size of the node in bytes.
func (node BNode) nodeSizeBytes() uint16 {
	return node.getKVPos(node.getNumKeys())
}
