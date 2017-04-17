package jsonpatch

//Patch is the spec for a json-patch operation : http://jsonpatch.com/
type Patch []PatchItem

//New returns a new Patch list
func New() Patch {
	return make([]PatchItem, 0)
}

//Add creats and adds an item to the patch
func (p Patch) Add(op string, path string, value interface{}) Patch {
	p = append(p, NewItem(op, path, value))
	return p
}

//PatchItem is a single item in Patch
type PatchItem struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

//NewItem returns a new Patch item
func NewItem(op string, path string, value interface{}) PatchItem {
	return PatchItem{
		Op:    op,
		Path:  path,
		Value: value,
	}
	//TODO: Maybe do some validations?
}
