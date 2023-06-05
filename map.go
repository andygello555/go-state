package state

import (
	"fmt"
	"github.com/andygello555/gotils/v2/slices"
	"github.com/pkg/errors"
	"strings"
)

type linkedMapValue[K comparable, V any] struct {
	Key    K       `json:"key"`
	Value  V       `json:"value"`
	Next   *uint64 `json:"next"`
	Head   *uint64 `json:"head"`
	Tail   *uint64 `json:"tail"`
	User   bool    `json:"user"`
	Length int     `json:"length"`
}

type mapProto[K comparable, V any] struct {
	vals   map[uint64]V
	hashes map[uint64]K
	keys   map[K]*hashKey
	name   string
	enc    EncodingType
	zero   *hashKey
}

func (m *mapProto[K, V]) signature() string {
	return fmt.Sprintf("%s (map[%T]%T)", m.name, *new(K), *new(V))
}

func (m *mapProto[K, V]) setRegardless(writer StateWriter, key *hashKey, value *linkedMapValue[K, V]) error {
	return m.enc.serialise(writer, key, value)
}

func (m *mapProto[K, V]) getRegardless(reader StateReader, key *hashKey) (value linkedMapValue[K, V], err error) {
	if err = m.enc.deserialise(reader, key, &value); err != nil {
		err = errors.Wrapf(err, "getting %X from %s", key.h, m.signature())
	}
	return
}

func (m *mapProto[K, V]) get(reader StateReader, key *hashKey) (value any, err error) {
	var ok bool
	if value, ok = m.vals[key.h]; !ok {
		// If there isn't we need to load the value from disk
		var lVal linkedMapValue[K, V]
		if lVal, err = m.getRegardless(reader, key); err != nil {
			return
		}
		value = lVal.Value
	}
	return
}

func (m *mapProto[K, V]) createZeroKey(writer StateWriter) (err error) {
	if m.zero == nil {
		var (
			zeroKey   K
			hashedKey *hashKey
		)

		if hashedKey, err = m.hashFromComponents(zeroKey); err != nil {
			return
		}
		defer func() {
			if err == nil {
				m.zero = hashedKey
			}
		}()

		if !writer.FSWriter().Exists(hashedKey.path()) {
			if err = m.setRegardless(writer, hashedKey, &linkedMapValue[K, V]{}); err != nil {
				err = errors.Wrapf(err, "could not create zero key for %s", m.signature())
			}
		}
	}
	return
}

func (m *mapProto[K, V]) getZeroKey(state StateReadWriter) (value linkedMapValue[K, V], err error) {
	if err = m.createZeroKey(state); err != nil {
		return
	}

	if value, err = m.getRegardless(state, m.zero); err != nil {
		err = errors.Wrap(err, "zero key")
	}
	return
}

func (m *mapProto[K, V]) checkKeyType(key any) (keyTyped K, err error) {
	var ok bool
	if keyTyped, ok = key.(K); !ok {
		err = fmt.Errorf("key for %s is not %T", m.signature(), *new(K))
	}
	return
}

func (m *mapProto[K, V]) getOrCreateHashKey(key K) (hashedKey *hashKey) {
	var ok bool
	if hashedKey, ok = m.keys[key]; !ok {
		// If we can't find a cached hash for this key, then we will create a new hashKey and cache it in the keys map
		hashedKey = emptyKey(m)
		hashedKey.addComponent(key)
		m.keys[key] = hashedKey
	}
	return
}

func (m *mapProto[K, V]) hashFromComponents(components ...any) (hashedKey *hashKey, err error) {
	if len(components) != 1 {
		err = fmt.Errorf("there should only be one component to Get from %s", m.signature())
		return
	}

	var keyTyped K
	if keyTyped, err = m.checkKeyType(components[0]); err != nil {
		return
	}
	hashedKey = m.getOrCreateHashKey(keyTyped)

	// Find the hash code of the hashedKey fetched above
	if _, err = hashedKey.hash(); err != nil {
		err = errors.Wrapf(err, "cannot find hash of hashKey %v", hashedKey)
		return
	}

	m.hashes[hashedKey.h] = keyTyped
	return
}

func (m *mapProto[K, V]) componentFromHash(reader StateReader, key *hashKey) (component K, err error) {
	var (
		hash uint64
		ok   bool
	)

	if hash, err = key.hash(); err != nil {
		err = errors.Wrapf(err, "cannot find hash of hashKey %v", key)
		return
	}

	if component, ok = m.hashes[hash]; !ok {
		// We need to load the file for the key from disk to find the components
		var value linkedMapValue[K, V]
		if value, err = m.getRegardless(reader, key); err != nil {
			err = errors.Wrapf(err, "cannot find components for hashKey %v from disk", key)
			return
		}
		component = value.Key
		m.hashes[hash] = component
	}
	return
}

func (m *mapProto[K, V]) String(reader StateReader) (out string) {
	var (
		b   strings.Builder
		err error
	)

	b.WriteString("map[")
	defer func() {
		if err != nil {
			b.WriteString(fmt.Sprintf("%%!(%v)", err))
		}
		b.WriteString("]")
		out = b.String()
	}()

	var iter CachedFieldIterator
	if iter, err = m.Iter(reader); err != nil {
		return
	}

	keys := make([]K, iter.Len())
	for iter.Continue() {
		keys[iter.I()] = iter.Components()[0].(K)
		if err = iter.Next(); err != nil {
			return
		}
	}
	slices.Order(keys)

	for keyNo, key := range keys {
		var val any
		if val, err = m.Get(reader, key); err != nil {
			return
		}
		b.WriteString(fmt.Sprintf("%v:%v", key, val))
		if keyNo < len(keys)-1 {
			b.WriteString(", ")
		}
	}
	return
}

func (m *mapProto[K, V]) Name() string { return m.name }

func (m *mapProto[K, V]) Encoding() EncodingType { return m.enc }

func (m *mapProto[K, V]) Set(writer StateWriter, value any, components ...any) (err error) {
	var (
		state    StateReadWriter
		ok       bool
		typedVal V
		newKey   *hashKey
		newVal   linkedMapValue[K, V]
		zeroVal  linkedMapValue[K, V]
	)

	if state, ok = writer.(StateReadWriter); !ok {
		return fmt.Errorf("cannot set value for %s as State doesn't implement StateReadWriter", m.signature())
	}

	if typedVal, ok = value.(V); !ok {
		return fmt.Errorf("value for %s is not %T", m.signature(), *new(V))
	}

	if newKey, err = m.hashFromComponents(components...); err != nil {
		return
	}

	if zeroVal, err = m.getZeroKey(state); err != nil {
		return
	}

	// Overwrite the value at this hash code
	m.vals[newKey.h] = typedVal

	newVal.User = true
	newVal.Value = typedVal
	newVal.Key = newKey.components[0].(K)

	// Keep track of the values we need to write
	const (
		New = iota
		Zero
		Previous
	)

	toWrite := []struct {
		name  string
		key   *hashKey
		val   *linkedMapValue[K, V]
		write bool
	}{
		{name: "new key", key: newKey, val: &newVal},
		{name: "zero key", key: m.zero, val: &zeroVal},
		{name: "previous tail", key: nil, val: nil},
	}

	if writer.FSWriter().Exists(newKey.path()) && newKey.h != m.zero.h {
		// Overwrite the existing linkedMapValue at the hash (1 file is written)
		toWrite[New].name = "overwritten key"
		toWrite[New].write = true

		if newVal, err = m.getRegardless(state, newKey); err != nil {
			return errors.Wrapf(
				err, "could not read value for existing key %X from %s",
				newKey.h, m.signature(),
			)
		}
	} else {
		// Create a new value, or overwrite the zero key (1, 2, or 3 files are written)
		toWrite[Zero].write = true
		if newKey.h == m.zero.h {
			toWrite[Zero].name = "overwritten zero key"
			zeroVal.Value = typedVal
		} else {
			toWrite[New].write = true
		}

		previousTail := zeroVal.Tail
		zeroVal.Tail = &newKey.h
		zeroVal.Length++
		if zeroVal.Head == nil {
			zeroVal.Head = &newKey.h
		}

		if previousTail != nil {
			if *previousTail == m.zero.h {
				zeroVal.Next = &newKey.h
			} else {
				var previousVal linkedMapValue[K, V]
				toWrite[Previous].key = unawareKey(*previousTail, m)
				if previousVal, err = m.getRegardless(state, toWrite[Previous].key); err != nil {
					err = errors.Wrap(err, "previous tail")
					return
				}

				previousVal.Next = &newKey.h
				toWrite[Previous].val = &previousVal
				toWrite[Previous].write = true
			}
		}
	}

	// Write all the files we need to
	for _, write := range toWrite {
		if !write.write {
			continue
		}

		if err = m.setRegardless(writer, write.key, write.val); err != nil {
			err = errors.Wrapf(err, "setting %X (%s) in %s", write.key.h, write.name, m.signature())
			return
		}
	}
	return
}

func (m *mapProto[K, V]) Get(state StateReader, components ...any) (value any, err error) {
	var hashedKey *hashKey
	if hashedKey, err = m.hashFromComponents(components...); err != nil {
		return
	}
	return m.get(state, hashedKey)
}

func (m *mapProto[K, V]) Make() CachedField {
	return &mapProto[K, V]{
		vals:   make(map[uint64]V),
		hashes: make(map[uint64]K),
		keys:   make(map[K]*hashKey),
		name:   m.name,
		enc:    m.enc,
	}
}

func (m *mapProto[K, V]) Iter(reader StateReader) (CachedFieldIterator, error) {
	var (
		state StateReadWriter
		ok    bool
	)
	if state, ok = reader.(StateReadWriter); !ok {
		return nil, fmt.Errorf("cannot create iterator for %s as State doesn't implement StateReadWriter", m.signature())
	}

	iter := mapIterator[K, V]{
		m:      m,
		reader: reader,
	}

	zeroVal, err := m.getZeroKey(state)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot determine length and start of %s", m.signature())
	}

	iter.length = zeroVal.Length
	if zeroVal.Head != nil {
		var component K
		iter.current = unawareKey(*zeroVal.Head, m)
		if *zeroVal.Head != m.zero.h {
			if component, err = m.componentFromHash(reader, iter.current); err != nil {
				return nil, err
			}
		}
		iter.component = component
	}
	return &iter, nil
}

func (m *mapProto[K, V]) Merge(mergeTo StateWriter, mergeFrom StateReader, fieldToMerge IterableCachedField) (err error) {
	var fieldToMergeIter CachedFieldIterator
	if fieldToMergeIter, err = fieldToMerge.Iter(mergeFrom); err != nil {
		return
	}

	for fieldToMergeIter.Continue() {
		var value any
		if value, err = fieldToMergeIter.Get(); err != nil {
			return
		}

		if err = m.Set(mergeTo, value, fieldToMergeIter.Components()...); err != nil {
			return
		}

		if err = fieldToMergeIter.Next(); err != nil {
			return
		}
	}
	return
}

func Map[K comparable, V any](name string, encoding EncodingType) CachedField {
	if name == "" {
		name = fmt.Sprintf("map[%T]%T-%s", *new(K), *new(V), encoding.String())
	}
	return &mapProto[K, V]{name: name, enc: encoding}
}

type mapIterator[K comparable, V any] struct {
	i         int
	m         *mapProto[K, V]
	reader    StateReader
	current   *hashKey
	component K
	length    int
}

func (mi *mapIterator[K, V]) Continue() bool              { return mi.current != nil }
func (mi *mapIterator[K, V]) I() int                      { return mi.i }
func (mi *mapIterator[K, V]) Components() []any           { return []any{mi.component} }
func (mi *mapIterator[K, V]) Field() IterableCachedField  { return mi.m }
func (mi *mapIterator[K, V]) Len() int                    { return mi.length }
func (mi *mapIterator[K, V]) Get() (value any, err error) { return mi.m.get(mi.reader, mi.current) }

func (mi *mapIterator[K, V]) Next() (err error) {
	if mi.current != nil {
		var current linkedMapValue[K, V]
		if current, err = mi.m.getRegardless(mi.reader, mi.current); err != nil {
			return
		}

		var component K
		mi.current = nil
		if current.Next != nil {
			mi.current = unawareKey(*current.Next, mi.m)
			component, _ = mi.m.componentFromHash(mi.reader, mi.current)
		}
		mi.component = component
		mi.i++
	}
	return
}
