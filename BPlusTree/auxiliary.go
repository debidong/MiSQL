package BPlusTree

import (
	"bytes"
	"encoding/binary"
)

// keyPosLookup finds the first position for a key in a node, and returns the index of it.
// It works for both non-leaf nodes and leaf nodes.
// TODO: binary search
func keyPosLookup(node BNode, key []byte) uint16 {
	numKeys := node.getNumKeys()
	index := uint16(0)
	for i := uint16(1); i < numKeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			index = i
		} else {
			break
		}
	}
	return index
}

// appendKVRange copies a range of KVs from a old node to a new node, and updates the offset list and pointers in new
// node as well. It copies KVs from the old node with the index of [srcBegin:srcBegin+rangeNum] to the new node with the index
// of [dstBegin:dstBegin+rangeNum]
// The caller is responsible for updating the header for the new node.
func appendKVRange(new BNode, old BNode, dstBegin uint16, srcBegin uint16, rangeNum uint16) {
	if rangeNum == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < rangeNum; i++ {
		new.setPtr(dstBegin+i, old.getPtr(srcBegin+i))
	}

	// offsets
	offsetDstBegin := new.getOffset(dstBegin)
	offsetSrcBegin := old.getOffset(srcBegin)
	for i := uint16(1); i <= rangeNum; i++ {
		offset := offsetDstBegin + (old.getOffset(srcBegin+i) - offsetSrcBegin)
		new.setOffset(dstBegin+i, offset)
	}
	//KVs
	begin := old.getKVPos(srcBegin)
	end := old.getKVPos(srcBegin + rangeNum)
	copy(new[new.getKVPos(dstBegin):], old[begin:end])
	return
}

// appendSingleKV inserts a KV pair into specific position in a node.
// The caller is responsible for updating the header for the new node.
func appendSingleKV(node BNode, dstIdx uint16, ptr uint64, key []byte, val []byte) {
	// pointer
	node.setPtr(dstIdx, ptr)
	// KV
	pos := node.getKVPos(dstIdx)
	binary.LittleEndian.PutUint16(node[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(node[pos+2:], uint16(len(val)))
	copy(node[pos+4:], key)
	copy(node[pos+4+uint16(len(key)):], val)
	// offset of NEXT KV
	node.setOffset(dstIdx+1, node.getOffset(dstIdx)+uint16(4+len(key)+len(val)))
}

func nodeUpdateAndReplace(tree *BPlusTree, new BNode, old BNode, index uint16, kids ...BNode) {
	new.setHeader(BNODE_INTERNAL, old.getNumKeys()+uint16(len(kids))-1)
	appendKVRange(new, old, 0, 0, index)
	for i, kid := range kids {
		appendSingleKV(new, index+uint16(i), tree.new(kid), kid.getKey(0), nil) // val of internal node is 0
	}
	appendKVRange(new, old, index+uint16(len(kids)), index+1, old.getNumKeys()-(index+1))
}
