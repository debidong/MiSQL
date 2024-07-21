package BPlusTree

import (
	"bytes"
)

/*
NODES ARE IMMUTABLE IN ORDER TO REALIZE CONCURRENCY.
ALL UPDATING OPERATIONS ARE NOT DONE IN-PLACE, BY DUPLICATING NEW DATA STRUCTURES INSTEAD.
*/

func (tree *BPlusTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node
		root := make(BNode, BTREE_PAGE_SIZE)
		root.setHeader(BNODE_LEAF, 2)
		appendSingleKV(root, 0, 0, nil, nil) // dummy key
		appendSingleKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	root := tree.get(tree.root)
	tree.del(tree.root)
	new := kvInsert(tree, root, key, val)
	nSplit, split := nodeSplit3(new)

	if nSplit == 1 {
		tree.root = tree.new(split[0])
		return
	}
	// else, the new root needs to be split
	root = make(BNode, BTREE_PAGE_SIZE)
	root.setHeader(BNODE_INTERNAL, nSplit)
	for i, kid := range split {
		appendSingleKV(root, uint16(i), tree.new(kid), kid.getKey(0), nil)
	}
	tree.root = tree.new(root)
	return
}

// kvInsert inserts a KV pair into a node. If the size of the node is too large to be fit into one page,
// the node might be split into 2 nodes.
// Note that the returned node obtained by the final recursion does not check whether the size is compliant. The caller
// of the function is responsible to check whether the node needs to be split.
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

func intrnNodeInsert(tree *BPlusTree, new BNode, node BNode, index uint16, key []byte, val []byte) {
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
}

func leafUpdate(new BNode, old BNode, index uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.getNumKeys())
	appendKVRange(new, old, 0, 0, index)
	appendSingleKV(new, index, 0, key, val)
	appendKVRange(new, old, index+1, index+1, old.getNumKeys()-index-1)
}

// nodeSplit3 splits a node into 3 kid nodes, making sure each of them fits into a page.
func nodeSplit3(node BNode) (uint16, [3]BNode) {
	if node.nodeSizeBytes() <= BTREE_PAGE_SIZE {
		node = node[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{node}
	}

	left := make(BNode, BTREE_PAGE_SIZE)
	right := make(BNode, 2*BTREE_PAGE_SIZE)
	nodeSplit2(left, right, node)
	if right.nodeSizeBytes() <= BTREE_PAGE_SIZE {
		right = right[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	left_ := make(BNode, BTREE_PAGE_SIZE)
	right_ := make(BNode, BTREE_PAGE_SIZE)
	nodeSplit2(left_, right_, right)
	return 3, [3]BNode{left, left_, right_}
}

// nodeSplit2 splits a node into two kid nodes, and makes sure that left node fits into one page. The right node may
// not, so it's the caller's responsible to split the oversize node again.
func nodeSplit2(left, right, node BNode) {
	var idx uint16
	for idx = 1; idx < node.getNumKeys(); idx++ {
		lenLeft := HEADER + (8+2+4)*idx + node.getOffset(idx)
		if lenLeft > BTREE_PAGE_SIZE {
			break
		}
	}
	idx = idx - 1

	// handle left node
	left.setHeader(left.getNodeType(), idx)
	appendKVRange(left, node, 0, 0, idx)
	// handle right node
	right.setHeader(right.getNodeType(), node.getNumKeys()-idx)
	appendKVRange(right, node, 0, idx, node.getNumKeys()-idx)
}
