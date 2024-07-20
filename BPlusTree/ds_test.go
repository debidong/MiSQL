package BPlusTree

import (
	"testing"
	"unsafe"
)

type C struct {
	tree  BPlusTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BPlusTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				if ok {
					return node
				}
				return BNode{}
			},
			new: func(node BNode) uint64 {
				ptr := uint64(uintptr(unsafe.Pointer(&node)))
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
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
		val := string(c.tree.Get([]byte(k)))
		if v != val {
			t.Errorf("Failed, %s is not equal to %s", v, val)
		}
	}
}

func TestBPlusTree_Update(t *testing.T) {
	c := newC()
	c.add("paul", "mccartney")
	c.add("john", "lennon")
	c.add("ringo", "starr")
	c.add("george", "harrison")

	c.add("john", "mayer")

	updated := c.ref["john"]
	val := string(c.tree.Get([]byte("john")))
	if updated != val {
		t.Errorf("Failed, %s is not equal to %s", updated, val)
	}
}

func TestBPlusTree_Delete(t *testing.T) {

}
