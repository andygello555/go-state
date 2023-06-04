package state

const keyPoolSize = 50

// CachedField represents a field in stateProto that can be cached on-disk using the PathsToBytesDirectory lookup.
type CachedField interface {
	Name() string
	Encoding() EncodingType
	Set(state StateWriter, value any, components ...any) error
	Get(state StateReader, components ...any) (any, error)
	Make() CachedField
}

// IterableCachedField represents a field in stateProto that can be cached, but also can be iterated through using the
// CachedFieldIterator.
type IterableCachedField interface {
	CachedField
	// Iter returns the CachedFieldIterator that can be used to iterate over the instance of the CachedField itself.
	Iter(state StateReader) (CachedFieldIterator, error)
	// Merge will perform a union/update procedure between the referred to IterableCachedField and the given
	// CachedField.
	Merge(mergeTo StateWriter, mergeFrom StateReader, fieldToMerge IterableCachedField) error
}

type CachedFieldStringer interface {
	String(state StateReader) string
}

type CachedFieldIterator interface {
	// Continue checks whether the CachedFieldIterator has not finished. I.e. there are still more elements to iterate over.
	Continue() bool
	// I will return the current i (index) value.
	I() int
	// Components will return the key components for the current element that can be passed to CachedField.Get to
	// retrieve it.
	Components() []any
	// Get will return the current element that is being iterated over.
	Get() (any, error)
	// Next should be called at the end of each loop to retrieve the next element.
	Next() error
	// Field will return the IterableCachedField that this CachedFieldIterator is iterating over.
	Field() IterableCachedField
	// Len returns the length of the CachedFieldIterator.
	Len() int
}

type mergedCachedFieldIterator struct {
	its       []CachedFieldIterator
	idx       int
	completed int
}

func (m *mergedCachedFieldIterator) Continue() bool {
	// If the current iterator can continue, or the next iterator can continue (if there is one)
	if len(m.its) == 0 {
		return false
	} else if m.its[m.idx].Continue() {
		return true
	} else if m.idx < len(m.its)-1 && m.its[m.idx+1].Continue() {
		// We increment the current iterator ptr if there is another iterator that has not yet been started
		m.completed += m.its[m.idx].Len()
		m.idx++
		return true
	}
	return false
}

func (m *mergedCachedFieldIterator) I() int                     { return m.completed + m.its[m.idx].I() }
func (m *mergedCachedFieldIterator) Components() []any          { return m.its[m.idx].Components() }
func (m *mergedCachedFieldIterator) Get() (any, error)          { return m.its[m.idx].Get() }
func (m *mergedCachedFieldIterator) Next() error                { return m.its[m.idx].Next() }
func (m *mergedCachedFieldIterator) Field() IterableCachedField { return m.its[m.idx].Field() }

func (m *mergedCachedFieldIterator) Len() int {
	length := 0
	for _, field := range m.its {
		length += field.Len()
	}
	return length
}

func MergeCachedFieldIterators(iterators ...CachedFieldIterator) CachedFieldIterator {
	return &mergedCachedFieldIterator{
		its: iterators,
		idx: 0,
	}
}

//type mapOfArraysProto[K comparable, E any] struct {
//	Inner       map[K][]E
//	BaseDirName string
//	Encoding    EncodingType
//	StringKey   bool
//}
//
//func (m *mapOfArraysProto[K, E]) String() string {
//	keys := maps.Keys(m.Inner)
//	slices.Order(keys)
//
//	var b strings.Builder
//	b.WriteString("map[")
//	for keyNo, key := range keys {
//		array := m.Inner[key]
//		b.WriteString(fmt.Sprintf("%v:%v", key, array))
//		if keyNo < len(keys)-1 {
//			b.WriteString(" ")
//		}
//	}
//	b.WriteString("]")
//	return b.String()
//}
//
//func (m *mapOfArraysProto[K, E]) BaseDir() string { return m.BaseDirName }
//
//func (m *mapOfArraysProto[K, E]) path(paths ...string) string {
//	return filepath.Join(m.BaseDir(), strings.Join(paths, "_")+m.Encoding.Ext())
//}
//
//func (m *mapOfArraysProto[K, E]) keyToString(key K) string {
//	data, _ := json.Marshal(&key)
//	return string(data)
//}
//
//func (m *mapOfArraysProto[K, E]) keyToUnescapedString(key K) string {
//	s := m.keyToString(key)
//	if m.StringKey {
//		if us, err := strconv.Unquote(m.keyToString(key)); err == nil {
//			return us
//		}
//	}
//	return s
//}
//
//func (m *mapOfArraysProto[K, E]) keyFromString(s string) K {
//	if m.StringKey {
//		s = fmt.Sprintf("%q", s)
//	}
//
//	var key K
//	_ = json.Unmarshal([]byte(s), &key)
//	return key
//}
//
//func (m *mapOfArraysProto[K, E]) Serialise(writer PathsToBytesWriter) (err error) {
//	// We create an info file so that BaseDir is always created
//	if err = writer.AddFilenameBytes(
//		filepath.Join(m.BaseDir(), "meta.json"),
//		[]byte(fmt.Sprintf(
//			"{\"type\": \"map[%T][]%T\", \"count\": %d, \"keys\": [%s]}",
//			*new(K),
//			*new(E),
//			m.Len(),
//			strings.Join(slices.Comprehension(maps.Keys(m.Inner), func(idx int, value K, arr []K) string {
//				return m.keyToString(value)
//			}), ", "),
//		)),
//	); err != nil {
//		return errors.Wrapf(err, "could not add meta.json to serialised directory for %s", m.BaseDir())
//	}
//
//	for key, array := range m.Inner {
//		var data []byte
//		switch m.Encoding {
//		case JSON:
//			if data, err = json.Marshal(&array); err != nil {
//				return errors.Wrapf(
//					err, "could not serialise slice of %d elements for key %v to %s",
//					len(array), key, m.Encoding,
//				)
//			}
//		case Gob:
//			var buf bytes.Buffer
//			if err = gob.NewEncoder(&buf).Encode(array); err != nil {
//				return errors.Wrapf(err, "could not serialise slice of %d elements for key %v to %s", len(array), key, m.Encoding)
//			}
//			data = buf.Bytes()
//		default:
//			return fmt.Errorf("cannot serialise to %s", m.Encoding)
//		}
//
//		if err = writer.AddFilenameBytes(m.path(m.keyToUnescapedString(key)), data); err != nil {
//			return
//		}
//	}
//	return
//}
//
//func (m *mapOfArraysProto[K, E]) Deserialise(reader PathsToBytesReader) (err error) {
//	ftb := reader.BytesForDirectory(m.BaseDir())
//	for filename, data := range ftb.inner {
//		if filepath.Base(filename) != "meta.json" {
//			// Trim the PathToBytes BaseDir, the DeveloperSnapshots BaseDir, and the file extension
//			key := m.keyFromString(strings.TrimSuffix(filepath.Base(filename), m.Encoding.Ext()))
//
//			var array []E
//			switch m.Encoding {
//			case JSON:
//				if err = json.Unmarshal(data, &array); err != nil {
//					return errors.Wrapf(err, "could not deserialise slice for key %v", key)
//				}
//			case Gob:
//				if err = gob.NewDecoder(bytes.NewReader(data)).Decode(&array); err != nil {
//					return errors.Wrapf(err, "could not deserialise slice for key %v", key)
//				}
//			default:
//				return fmt.Errorf("cannot deserialise from %s", m.Encoding)
//			}
//
//			if _, ok := m.Inner[key]; !ok {
//				m.Inner[key] = make([]E, len(array))
//				copy(m.Inner[key], array)
//			} else {
//				m.Inner[key] = append(m.Inner[key], array...)
//			}
//		}
//	}
//	ftb = nil
//	return
//}
//
//func (m *mapOfArraysProto[K, E]) Set(value any, components ...any) {
//	for argNo := 0; argNo < len(args); argNo += 2 {
//		key := args[argNo].(K)
//		if _, ok := m.Inner[key]; !ok {
//			m.Inner[key] = make([]E, 0)
//		}
//		m.Inner[key] = append(m.Inner[key], args[argNo+1].(E))
//	}
//}
//
//func (m *mapOfArraysProto[K, E]) Get(key any) (value any, ok bool) {
//	value, ok = m.Inner[key.(K)]
//	return
//}
//
//func (m *mapOfArraysProto[K, E]) Make() CachedField {
//	return &mapOfArraysProto[K, E]{
//		Inner:       make(map[K][]E),
//		BaseDirName: m.BaseDirName,
//		Encoding:    m.Encoding,
//		StringKey:   m.StringKey,
//	}
//}
//
//func (m *mapOfArraysProto[K, E]) Len() int { return len(m.Inner) }
//
//func (m *mapOfArraysProto[K, E]) Iter() CachedFieldIterator {
//	iter := &mapIterator{
//		cachedField: m,
//		queue:       make([]any, m.Len()),
//	}
//
//	i := 0
//	for key := range m.Inner {
//		iter.queue[i] = key
//		i++
//	}
//	return iter
//}
//
//func (m *mapOfArraysProto[K, E]) Merge(field CachedField) {
//	for key, array := range field.(*mapOfArraysProto[K, E]).Inner {
//		if _, ok := m.Inner[key]; !ok {
//			m.Inner[key] = make([]E, len(array))
//			copy(m.Inner[key], array)
//		} else {
//			m.Inner[key] = append(m.Inner[key], array...)
//		}
//	}
//}
//
//func MapOfArrays[K comparable, E any](baseDir string, encoding EncodingType) CachedField {
//	if baseDir == "" {
//		baseDir = fmt.Sprintf("map[%T][]%T", *new(K), *new(E))
//	}
//	return &mapOfArraysProto[K, E]{
//		BaseDirName: baseDir,
//		Encoding:    encoding,
//		StringKey:   reflect.TypeOf(new(K)).Elem() == reflect.TypeOf(""),
//	}
//}
