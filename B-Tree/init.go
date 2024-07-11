package B_Tree

const (
	HEADER = 4 // size of header of BNode

	BTREE_PAGE_SIZE    = 8 * 1024
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	maxNodeLength := HEADER + 1*2 + 1*8 + (1*4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE)
	if !(maxNodeLength < BTREE_PAGE_SIZE) {
		panic("A node must be able to fit into one page.")
	}
}
