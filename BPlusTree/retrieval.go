package BPlusTree

import "bytes"

func (tree *BPlusTree) GetVal(key []byte) ([]byte, bool) {
	root := tree.Get(tree.Root)
	return getVal(tree, root, key)
}

func getVal(tree *BPlusTree, node Node, key []byte) ([]byte, bool) {
	idx := keyPosLookup(node, key)
	switch node.getNodeType() {
	case BNODE_INTERNAL:
		node = tree.Get(node.getPtr(idx))
		return getVal(tree, node, key)
	case BNODE_LEAF:
		if bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		} else {
			return make([]byte, 0), false
		}
	default:
		return make([]byte, 0), false
	}
}
