package BPlusTree

import (
	"bytes"
)

/*
NODES ARE IMMUTABLE IN ORDER TO REALIZE CONCURRENCY.
ALL UPDATING OPERATIONS ARE NOT DONE IN-PLACE, BY DUPLICATING NEW DATA STRUCTURES INSTEAD.
*/

// kvInsert inserts a KV pair into a node. If the size of the node is too large to be fit into one page,
// the node might be split into 2 nodes.
func kvInsert(tree *BPlusTree, node BNode, key []byte, val []byte) BNode {
	new := make([]byte, 2*BTREE_PAGE_SIZE)
	index := keyPosLookup(node, key)

	switch node.getNodeType() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(index)) {
			// update the new val to the leaf node
			leafUpdate(new, node, index, key, val)
		} else {
			// insert the new node
			leafInsert(new, node, index+1, key, val)
		}
	case BNODE_INTERNAL:
		// recursive insertion to the node
		intrnNodeInsert(tree, new, node, index+1, key, val)
	default:
		// untyped node
		// TODO: error handling for untyped node, consider using ErrUntypedNode
		return make([]byte, 0)
	}
	return new
}

func leafInsert(new BNode, old BNode, index uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.getNumKeys()+1)
	appendKVRange(new, old, 0, 0, index)
	appendSingleKV(new, index, 0, key, val) // pointer should be set to 0 since we are inserting TERMINAL nodes.
	appendKVRange(new, old, index+1, index, old.getNumKeys()-index)
}

func intrnNodeInsert(tree *BPlusTree, new BNode, node BNode, index uint16, key []byte, val []byte) error {
	// deallocate the old node
	keyPtr := node.getPtr(index)
	keyNode := tree.get(node.getPtr(index))
	tree.del(keyPtr)
	// recursive lookup and insertion
	keyNode = kvInsert(tree, keyNode, key, val)
	// split the node if needed
	numSplit, split := nodeSplit3(keyNode)

	// reallocate modified duplicated kid nodes and update links from new node to them
	nodeUpdateAndReplace(tree, new, node, index, split[:numSplit]...)
	return nil
}

// TODO:
func leafUpdate(new BNode, old BNode, index uint16, key []byte, val []byte) {}

func nodeSplit3(node BNode) (uint16, [3]BNode) {
	if node.nodeSizeBytes() <= BTREE_PAGE_SIZE {
		node = node[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{node}
	}

	var (
		left  BNode
		right BNode
	)
	left = make([]byte, 2*BTREE_PAGE_SIZE)
	right = make([]byte, BTREE_PAGE_SIZE)
	nodeSplit2(left, right, node)
	if left.nodeSizeBytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	var (
		left_  BNode
		right_ BNode
	)
	left_ = make([]byte, BTREE_PAGE_SIZE)
	nodeSplit2(left_, right_, left)
	return 3, [3]BNode{left_, right_, right}
}

// TODO:
func nodeSplit2(left, right, node BNode) {}

func nodeUpdateAndReplace(tree *BPlusTree, new BNode, old BNode, index uint16, kids ...BNode) {
	new.setHeader(BNODE_INTERNAL, old.getNumKeys()+uint16(len(kids))-1)
	appendKVRange(new, old, 0, 0, index)
	for i, kid := range kids {
		appendSingleKV(new, index+uint16(i), tree.new(kid), kid.getKey(0), nil) // val of internal node is 0
	}
	appendKVRange(new, old, index+uint16(len(kids)), index+1, old.getNumKeys()-(index+1))
}
