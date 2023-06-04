package state

import (
	"fmt"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"
)

const averageComponentsForKey = 3

type hashKey struct {
	components  []any
	field       CachedField
	h           uint64
	invalidated bool
}

func emptyKey(field CachedField) *hashKey {
	return &hashKey{components: make([]any, 0, averageComponentsForKey), field: field}
}

func unawareKey(h uint64, field CachedField) *hashKey {
	return &hashKey{h: h, field: field}
}

func (k *hashKey) hash() (uint64, error) {
	var err error
	if k.invalidated && len(k.components) > 0 {
		k.h, err = hashstructure.Hash(struct {
			FieldName  string
			Components []any
		}{
			FieldName:  k.field.Name(),
			Components: k.components,
		}, hashstructure.FormatV2, nil)
		k.invalidated = false
	}
	return k.h, err
}

func (k *hashKey) path() string {
	if h, err := k.hash(); err != nil {
		panic(errors.Wrapf(err, "cannot hash Components %v", k.components))
	} else {
		return fmt.Sprintf("%X%s", h, k.field.Encoding().Ext())
	}
}

func (k *hashKey) setComponent(i int, component any) {
	if len(k.components) > 0 && i >= 0 && i < len(k.components) {
		k.components[i] = component
		k.invalidated = true
	}
}

func (k *hashKey) addComponent(component any) {
	if k.components == nil {
		k.components = make([]any, averageComponentsForKey)
		k.components = k.components[:1]
		k.components[0] = component
	} else {
		k.components = append(k.components, component)
	}
	k.invalidated = true
}
