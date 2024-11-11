package bptree

import "errors"

const (
	BTNODE_HEADER      = 4 // size of header of Node
	PAGE_SIZE          = 4 * 1024
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

var (
	ErrUntypedNode = errors.New("node without a type")
)

func init() {
	maxNodeLength := BTNODE_HEADER + 1*2 + 1*8 + (1*4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE)
	if !(maxNodeLength < PAGE_SIZE) {
		panic("A node must be able to fit into one page.")
	}
}
