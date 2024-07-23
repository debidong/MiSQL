package BPlusTree

import "bytes"

func (tree *BPlusTree) Get(key []byte) ([]byte, bool) {
	root := tree.get(tree.root)
	return getVal(tree, root, key)
}

func getVal(tree *BPlusTree, node BNode, key []byte) ([]byte, bool) {
	idx := keyPosLookup(node, key)
	switch node.getNodeType() {
	case BNODE_INTERNAL:
		node = tree.get(node.getPtr(idx))
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
