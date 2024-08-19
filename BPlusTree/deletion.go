package BPlusTree

import "bytes"

func (tree *BPlusTree) Delete(key []byte) bool {
	if tree.Root == 0 {
		return false
	}
	root := tree.Get(tree.Root)
	new := kvDelete(tree, root, key)
	if new.getNodeType() == BNODE_INTERNAL && new.getNumKeys() == 1 {
		tree.Root = new.getPtr(0)
	} else {
		tree.Root = tree.New(new)
	}
	return true
}

func kvDelete(tree *BPlusTree, node Node, key []byte) Node {
	idx := keyPosLookup(node, key)
	switch node.getNodeType() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return Node{}
		}
		new := make(Node, PAGE_SIZE)
		leafDelete(new, node, idx)
		return new
	case BNODE_INTERNAL:
		return intrnNodeDelete(tree, node, idx, key)
	default:
		// untyped node
		return make(Node, 0)
	}
}

func leafDelete(new Node, old Node, index uint16) {
	new.setHeader(BNODE_LEAF, old.getNumKeys()-1)
	appendKVRange(new, old, 0, 0, index)
	appendKVRange(new, old, index, index+1, old.getNumKeys()-1-index)
}

func intrnNodeDelete(tree *BPlusTree, node Node, index uint16, key []byte) Node {
	keyPtr := node.getPtr(index)
	kidNode := tree.Get(keyPtr)
	// recursive lookup to reach the terminal node to be deleted
	kidNode = kvDelete(tree, kidNode, key)
	if len(kidNode) == 0 {
		return Node{}
	}
	tree.Del(keyPtr)

	new := make(Node, PAGE_SIZE) // new internal node
	// check for merging
	dir, sibling := nodeCheckMergeable(tree, kidNode, node, index)
	switch {
	case dir == 0:
		nodeUpdateAndReplace(tree, new, node, index, kidNode)
	case dir < 0:
		// kidNode should be merged to its left sibling
		merged := make(Node, PAGE_SIZE)
		nodeMerge(merged, sibling, kidNode)
		tree.Del(node.getPtr(index - 1))
		nodeReplace2Kid(new, node, index-1, tree.New(merged), merged.getKey(0))
	case dir > 0:
		// kidNode should be merged to its right sibling
		merged := make(Node, PAGE_SIZE)
		nodeMerge(merged, kidNode, sibling)
		tree.Del(node.getPtr(index + 1))
		nodeReplace2Kid(new, node, index, tree.New(merged), merged.getKey(0))
	}
	return new
}

// nodeCheckMergeable checks whether a node should be merged to its siblings, and returns the merging direction with
// its sibling, if mergeable.
// A node and its sibling are mergeable, if:
// 1. Size of the node is no greater than max_page_size after merging;
// 2. Size of the node is greater than max_page_size/4 before merging.
func nodeCheckMergeable(tree *BPlusTree, new Node, node Node, index uint16) (int, Node) {
	if new.nodeSizeBytes() > PAGE_SIZE/4 {
		return 0, Node{}
	}
	if index > 0 {
		// try to check mergeable with its left sibling first
		sibling := tree.Get(node.getPtr(index - 1))
		if sibling.nodeSizeBytes()+new.nodeSizeBytes()-BTNODE_HEADER < PAGE_SIZE {
			return -1, sibling
		}
	}
	if index+1 < node.getNumKeys() {
		// check mergeable with its right sibling then
		sibling := tree.Get(node.getPtr(index + 1))
		if sibling.nodeSizeBytes()+new.nodeSizeBytes()-BTNODE_HEADER < PAGE_SIZE {
			return 1, sibling
		}
	}
	return 0, Node{}
}

func nodeMerge(merged Node, left Node, right Node) {
	merged.setHeader(left.getNodeType(), left.getNumKeys()+right.getNumKeys())
	appendKVRange(merged, left, 0, 0, left.getNumKeys())
	appendKVRange(merged, right, left.getNumKeys(), 0, right.getNumKeys())
}

// nodeReplace2Kid updates the new node with merged node and the rest of kid nodes of the old node.
// It accepts a pointer and the key of the merged node, and the index of the left old kid node.
func nodeReplace2Kid(new Node, old Node, index uint16, merged uint64, key []byte) {
	new.setHeader(BNODE_INTERNAL, old.getNumKeys()-1)
	appendKVRange(new, old, 0, 0, index)
	appendSingleKV(new, index, merged, key, []byte{})
	appendKVRange(new, old, index+1, index+2, old.getNumKeys()-index-2)
}
