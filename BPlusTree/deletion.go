package BPlusTree

import "bytes"

func (tree *BPlusTree) Delete(key []byte) bool {
	if tree.root == 0 {
		return false
	}
	root := tree.get(tree.root)
	new := kvDelete(tree, root, key)
	if new.getNodeType() == BNODE_INTERNAL && new.getNumKeys() == 1 {
		tree.root = new.getPtr(0)
	} else {
		tree.root = tree.new(new)
	}
	return true
}

func kvDelete(tree *BPlusTree, node BNode, key []byte) BNode {
	idx := keyPosLookup(node, key)
	switch node.getNodeType() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		new := make(BNode, BTREE_PAGE_SIZE)
		leafDelete(new, node, idx)
		return new
	case BNODE_INTERNAL:
		return intrnNodeDelete(tree, node, idx, key)
	default:
		// untyped node
		// TODO: error handling for untyped node, consider using ErrUntypedNode
		return make(BNode, 0)
	}
}

func leafDelete(new BNode, old BNode, index uint16) {
	new.setHeader(BNODE_LEAF, old.getNumKeys()-1)
	appendKVRange(new, old, 0, 0, index)
	appendKVRange(new, old, index, index+1, old.getNumKeys()-1-index)
}

func intrnNodeDelete(tree *BPlusTree, node BNode, index uint16, key []byte) BNode {
	keyPtr := node.getPtr(index)
	keyNode := tree.get(keyPtr)
	// recursive lookup to reach the terminal node to be deleted
	keyNode = kvDelete(tree, keyNode, key)
	if len(keyNode) == 0 {
		return BNode{}
	}
	tree.del(keyPtr)

	new := make(BNode, BTREE_PAGE_SIZE) // new internal node
	// check for merging
	dir, sibling := nodeCheckMergeable(tree, keyNode, node, index)
	switch {
	case dir == 0:
		nodeUpdateAndReplace(tree, new, node, index, keyNode)
	case dir < 0:
		// keyNode should be merged to its left sibling
		merged := make(BNode, BTREE_PAGE_SIZE)
		nodeMerge(merged, sibling, keyNode)
		tree.del(node.getPtr(index - 1))
		nodeReplace2Kid(new, node, index, tree.new(merged), merged.getKey(0))
	case dir > 0:
		// keyNode should be merged to its right sibling
		merged := make(BNode, BTREE_PAGE_SIZE)
		nodeMerge(merged, keyNode, sibling)
		tree.del(node.getPtr(index + 1))
		nodeReplace2Kid(new, node, index, tree.new(merged), merged.getKey(0))
	}
	return new
}

// nodeCheckMergeable checks whether a node should be merged to its siblings, and returns the merging direction with
// its sibling, if mergeable.
// A node and its sibling are mergeable, if:
// 1. Size of the node is no greater than max_page_size after merging;
// 2. Size of the node is greater than max_page_size/4 before merging.
func nodeCheckMergeable(tree *BPlusTree, new BNode, node BNode, index uint16) (int, BNode) {
	if new.nodeSizeBytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if index > 0 {
		// try to check mergeable with its left sibling first
		sibling := tree.get(node.getPtr(index - 1))
		if sibling.nodeSizeBytes()+new.nodeSizeBytes()-HEADER < BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if index+1 < node.getNumKeys() {
		// check mergeable with its right sibling then
		sibling := tree.get(node.getPtr(index + 1))
		if sibling.nodeSizeBytes()+new.nodeSizeBytes()-HEADER < BTREE_PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, BNode{}
}

func nodeMerge(merged BNode, left BNode, right BNode) {
	merged.setHeader(left.getNodeType(), left.getNumKeys()+right.getNumKeys())
	appendKVRange(merged, left, 0, 0, left.getNumKeys())
	appendKVRange(merged, right, left.getNumKeys(), 0, right.getNumKeys())
}

// TODO:
func nodeReplace2Kid(new BNode, parent BNode, index uint16, merged uint64, key []byte) {

}
