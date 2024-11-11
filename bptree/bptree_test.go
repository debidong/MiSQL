package bptree

import (
	"testing"
	"unsafe"
)

type C struct {
	tree  BPlusTree
	ref   map[string]string
	pages map[uint64]Node
}

func newC() *C {
	pages := map[uint64]Node{}
	return &C{
		tree: BPlusTree{
			Get: func(ptr uint64) Node {
				node, ok := pages[ptr]
				if ok {
					return node
				}
				return Node{}
			},
			New: func(node Node) uint64 {
				ptr := uint64(uintptr(unsafe.Pointer(&node)))
				pages[ptr] = node
				return ptr
			},
			Del: func(ptr uint64) {
				_, ok := pages[ptr]
				if ok {
					delete(pages, ptr)
				}
				return
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func TestBPlusTree_Insert(t *testing.T) {
	c := newC()
	c.add("paul", "mccartney")
	c.add("john", "lennon")
	c.add("ringo", "starr")
	c.add("george", "harrison")

	for k, v := range c.ref {
		val, ok := c.tree.GetVal([]byte(k))
		if !ok {
			t.Errorf("Key %s not found", k)
		}
		if v != string(val) {
			t.Errorf("Failed, %s is not equal to %s", v, val)
		}
	}
}

func TestBPlusTree_Update(t *testing.T) {
	c := newC()
	c.add("paul", "mccartney")
	c.add("john", "lennon")
	c.add("ringo", "starr")
	//c.add("george", "harrison")

	c.add("john", "mayer")

	updated := c.ref["john"]
	val, ok := c.tree.GetVal([]byte("john"))

	if !ok {
		t.Errorf("key not found")
		return
	}

	if updated != string(val) {
		t.Errorf("Failed, %s is not equal to %s", updated, val)
	}
}

func TestBPlusTree_Delete(t *testing.T) {

}
