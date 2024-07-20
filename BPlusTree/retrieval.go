package BPlusTree

func (tree *BPlusTree) Get(key []byte) []byte {
	root := tree.get(tree.root)
	return getVal(tree, root, key)
}

func getVal(tree *BPlusTree, node BNode, key []byte) []byte {
	idx := keyPosLookup(node, key)
	switch node.getNodeType() {
	case BNODE_INTERNAL:
		node = tree.get(node.getPtr(idx))
		return getVal(tree, node, key)
	case BNODE_LEAF:
		return node.getVal(idx)
	default:
		return make([]byte, 0)
	}
}
