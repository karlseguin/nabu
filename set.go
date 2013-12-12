package nabu

import (
	"github.com/karlseguin/nabu/key"
)

// A set represents an index which has had a condition applied
type Set map[key.Type]interface{}

// An array of sets
type Sets []Set

// The number of items in our array of set
func (sets Sets) Len() int {
	return len(sets)
}

// Used to sort an array based on length
func (sets Sets) Less(i, j int) bool {
	return len(sets[i]) < len(sets[j])
}

// Used to sort an array based on length
func (sets Sets) Swap(i, j int) {
	x := sets[i]
	sets[i] = sets[j]
	sets[j] = x
}
