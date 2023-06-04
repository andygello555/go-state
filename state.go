package state

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	basePathDir    = "."
	basePathFormat = ".go-state-2006-01-02"
	binExtension   = ".bin"
	dirPerms       = 0777
	filePerms      = 0666
)

type State interface {
	// BaseDir returns the directory that the State will Save to.
	//
	// By default, if no override is set within the Blueprint, this is DefaultBaseDir.
	BaseDir() string
	// CreatedAt returns when the State was created.
	//
	// This is the first time that the State was saved to disk. State's that exist only in memory will return a zero
	// time.Time.
	CreatedAt() time.Time
	// Blueprint returns the Blueprint for the State.
	Blueprint() Blueprint
	// Get calls CachedField.Get on the CachedField with the given name.
	Get(fieldName string, components ...any) (any, error)
	// MustGet calls CachedField.Get on the CachedField with the given name, and panics if an error occurs.
	MustGet(fieldName string, components ...any) any
	// Set calls CachedField.Set on the CachedField with the given name.
	Set(fieldName string, value any, components ...any) error
	// MustSet calls CachedField.Set on the CachedField with the given name, and panics if an error occurs.
	MustSet(fieldName string, value any, components ...any)
	// Iter calls IterableCachedField.Iter on the IterableCachedField with the given name.
	Iter(fieldName string) (CachedFieldIterator, error)
	// MustIter calls IterableCachedField.Iter on the IterableCachedField with the given name, and panics if an error
	// occurs.
	MustIter(fieldName string) CachedFieldIterator
	// GetCachedField returns the CachedField of the given name.
	GetCachedField(fieldName string) CachedField
	// GetIterableCachedField returns the IterableCachedField of the given name.
	GetIterableCachedField(fieldName string) IterableCachedField
	// MergeIterableCachedFields merges the IterableCachedField(s) from the given State into the referred to State.
	MergeIterableCachedFields(stateToMerge State) error
	// Delete deletes the on-disk representation of the State. I.e. the directory residing at BaseDir.
	Delete()
	// String returns the string representation of the State.
	//
	// By default, if no override is set within the Blueprint, this is DefaultString.
	String() string
}

type StateReader interface {
	FSReader() FSReader
}

type StateWriter interface {
	FSWriter() FSWriter
}

type StateReadWriter interface {
	StateReader
	StateWriter
}

// stateProto represents the current state of the Scout procedure. It contains a number of CachedField that can be used
// to cache the stateProto persistently on-disk. All CachedField are saved within a directory that contains the date on
// which the stateProto was first created. The default behaviour for stateProto is to load the directory with the latest
// date-stamp, this can be done using the LoadNewest procedure.
type stateProto struct {
	bp           Blueprint
	createdAt    time.Time
	cachedFields map[string]CachedField
	// Whether stateProto was loaded from a previously cached stateProto.
	loaded bool
	// Whether the stateProto only exists in memory. Useful for a temporary stateProto that will be merged back into a
	// main stateProto.
	inMemory bool
}

func newState(blueprint Blueprint, loaded, forceCreate bool, createdAt time.Time) *stateProto {
	proto := blueprint.(*bpProto)
	state := &stateProto{
		// We create a copy of the Blueprint so that we don't affect mutate the Blueprint if it is global.
		bp: &bpProto{
			emptyFields:         proto.emptyFields,
			filename:            proto.filename,
			name:                proto.name,
			baseDirFunc:         proto.baseDirFunc,
			baseDirName:         proto.baseDirName,
			isBDFunc:            proto.isBDFunc,
			isBDName:            proto.isBDName,
			bothBDFuncAltered:   proto.bothBDFuncAltered,
			stringFunc:          proto.stringFunc,
			stringName:          proto.stringName,
			nowFunc:             proto.nowFunc,
			nowName:             proto.nowName,
			bpFnFunc:            proto.bpFnFunc,
			bpFnName:            proto.bpFnName,
			isBPFnFunc:          proto.isBPFnFunc,
			isBPFnName:          proto.isBPFnName,
			bothBPFnFuncAltered: proto.bothBPFnFuncAltered,
			computedNames:       proto.computedNames,
		},
		createdAt:    createdAt,
		cachedFields: proto.make(),
		loaded:       loaded,
	}

	if forceCreate {
		// If the cache directory on disk already exists we will remove it and create a new one
		state.Delete()
	}

	if !loaded {
		_ = os.MkdirAll(filepath.Join(basePathDir, state.BaseDir()), dirPerms)
		_ = state.bp.Set(state, nil)
	}
	return state
}

func (s *stateProto) BaseDir() string      { return s.bp.(*bpProto).baseDirFunc(s) }
func (s *stateProto) String() string       { return s.bp.(*bpProto).stringFunc(s) }
func (s *stateProto) CreatedAt() time.Time { return s.createdAt }
func (s *stateProto) Blueprint() Blueprint { return s.bp }

func (s *stateProto) Get(fieldName string, components ...any) (any, error) {
	return s.GetCachedField(fieldName).Get(s, components...)
}

func (s *stateProto) MustGet(fieldName string, components ...any) (value any) {
	var err error
	if value, err = s.Get(fieldName, components...); err != nil {
		panic(err)
	}
	return
}

func (s *stateProto) Set(fieldName string, value any, components ...any) error {
	return s.GetCachedField(fieldName).Set(s, value, components...)
}

func (s *stateProto) MustSet(fieldName string, value any, components ...any) {
	if err := s.Set(fieldName, value, components...); err != nil {
		panic(err)
	}
}

func (s *stateProto) Iter(fieldName string) (CachedFieldIterator, error) {
	if cf, ok := s.GetCachedField(fieldName).(IterableCachedField); ok {
		return cf.Iter(s)
	}
	return nil, fmt.Errorf("CachedField %q is not an IterableCachedField", fieldName)
}

func (s *stateProto) MustIter(fieldName string) CachedFieldIterator {
	cfi, err := s.Iter(fieldName)
	if err != nil {
		panic(err)
	}
	return cfi
}

func (s *stateProto) GetCachedField(fieldName string) CachedField {
	return s.cachedFields[fieldName]
}

func (s *stateProto) GetIterableCachedField(fieldName string) IterableCachedField {
	return s.GetCachedField(fieldName).(IterableCachedField)
}

func (s *stateProto) MergeIterableCachedFields(stateToMerge State) (err error) {
	if stateToMergeReader, ok := stateToMerge.(StateReader); !ok {
		return errors.New("state to merge does not implement StateReader")
	} else {
		for cachedFieldName, cachedField := range stateToMerge.(*stateProto).cachedFields {
			var iterableCachedField IterableCachedField
			if iterableCachedField, ok = cachedField.(IterableCachedField); ok {
				if err = s.GetIterableCachedField(cachedFieldName).Merge(s, stateToMergeReader, iterableCachedField); err != nil {
					err = errors.Wrapf(err, "could not merge field %s into State", cachedFieldName)
					return
				}
			}
		}
	}
	return
}

func (s *stateProto) Delete() {
	if !s.inMemory {
		if _, err := os.Stat(s.BaseDir()); !os.IsNotExist(err) {
			_ = os.RemoveAll(s.BaseDir())
		}
	}
}

func (s *stateProto) FSReader() FSReader {
	return DirFS(s.BaseDir())
}

func (s *stateProto) FSWriter() FSWriter {
	return DirFS(s.BaseDir())
}

// Load will load the State at the given path with the given Blueprint.
func Load(blueprint Blueprint, path string) (State, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not load State from %s", path)
	}

	if createdAt, ok := blueprint.(*bpProto).isBDFunc(stat); ok {
		state := newState(blueprint, true, false, createdAt)
		_, err = state.Get(blueprint.Name())
		return state, err
	}
	return nil, fmt.Errorf("could not load State from %s, BaseDir cannot be parsed", path)
}

// LoadNewest will either create a new State with the given Blueprint or load the newest State in the current directory,
//
// The newest State in the current directory is found by:
//  1. Iterating over all files
//  2. Checking if the filename can be parsed as a base directory for the Blueprint using the bound IsBaseDirFunc.
//  3. Checking if State.Load can be called with no errors for the current file/dirFS. This filters out any
//  3. Initialising the file/directory as a State of the Blueprint, then checking for the max State.CreatedAt in the
//     current directory.
//
// If a previous State is found in the current directory, then State.Load will automatically be called.
//
// The time used when creating a new State is found by using Blueprint.Now.
func LoadNewest(blueprint Blueprint) (state State, err error) {
	// Find the newest cache in the basePathDir
	var files []os.DirEntry
	if files, err = os.ReadDir(basePathDir); err != nil {
		err = errors.Wrapf(err, "could not read base directory \"%s\"", basePathDir)
		return
	}

	for _, f := range files {
		var info os.FileInfo
		if info, err = f.Info(); err != nil {
			err = nil
			continue
		}

		if createdAt, ok := blueprint.(*bpProto).isBDFunc(info); ok {
			newestState := newState(blueprint, true, false, createdAt)
			if _, err = newestState.Get(blueprint.Name()); err != nil {
				continue
			}

			if state == nil || newestState.CreatedAt().After(state.CreatedAt()) {
				state = newestState
			}
		}
	}

	if state == nil {
		// We create a new state for the current time if we didn't find a previous State.
		state = newState(blueprint, false, false, blueprint.Now())
	}
	return
}

// New creates a new State in-memory using the given Blueprint. This State can then be saved to disk using State.Save.
//
// If a State with the same Blueprint and the same State.BaseDir already exists on disk, then it will be deleted.
func New(blueprint Blueprint) State {
	return newState(blueprint, false, true, blueprint.Now())
}

// StateInMemory will create a stateProto that only exists in memory. Only the CachedField will be created and the
// stateProto.InMemory flag set.
func StateInMemory(blueprint Blueprint) State {
	return &stateProto{inMemory: true, cachedFields: blueprint.(*bpProto).make()}
}

// ScoutStateManager manages the reads/writes to and from a stateProto instance. It provides synchronisation across all
// goroutines that use a single stateProto via a sync.RWMutex.
type ScoutStateManager struct {
	state *stateProto
	mutex sync.RWMutex
}

// NewScoutStateManager creates a new ScoutStateManager and returns a pointer to it.
//func NewScoutStateManager(state *stateProto) *ScoutStateManager {
//	return &ScoutStateManager{
//		state: state,
//		mutex: sync.RWMutex{},
//	}
//}
//
//// Write locks the ScoutStateManager for writing and executes the given function with the internal stateProto passed in.
//func (man *ScoutStateManager) Write(fun func(state *stateProto) error) (err error) {
//	man.mutex.Lock()
//	defer man.mutex.Unlock()
//	if err = fun(man.state); err != nil {
//		err = errors.Wrap(err, "error occurred whilst writing to stateProto")
//	}
//	return
//}
//
//// Read locks the ScoutStateManager for reading and executes the given function with the internal stateProto passed in.
//func (man *ScoutStateManager) Read(fun func(state *stateProto) error) (err error) {
//	man.mutex.RLock()
//	defer man.mutex.RUnlock()
//	if err = fun(man.state); err != nil {
//		err = errors.Wrap(err, "error occurred whilst reading from stateProto")
//	}
//	return
//}
//
//// String returns the output of stateProto.String from within a Read scope.
//func (man *ScoutStateManager) String() (out string) {
//	_ = man.Read(func(state *stateProto) error { out = state.String(); return nil })
//	return
//}
//
//// Loaded returns stateProto.Loaded by fetching it within a Read scope.
//func (man *ScoutStateManager) Loaded() bool {
//	var loaded bool
//	_ = man.Read(func(state *stateProto) error { loaded = state.loaded; return nil })
//	return loaded
//}
//
//// InMemory returns a stateProto.InMemory by fetching it within a Read scope.
//func (man *ScoutStateManager) InMemory() (inMemory bool) {
//	_ = man.Read(func(state *stateProto) error { inMemory = state.inMemory; return nil })
//	return
//}
//
//// Save calls stateProto.Save within a Write scope. This is so that only one goroutine can save the inner stateProto at
//// once.
//func (man *ScoutStateManager) Save() error {
//	return man.Write(func(state *stateProto) error { return state.Save() })
//}
//
//// MergeIterableCachedFields calls ScoutState.MergeIterableCachedFields on the managed stateProto instance. It is
//// assumed that the given stateProto is not shared between multiple different goroutines and can be read safely.
//func (man *ScoutStateManager) MergeIterableCachedFields(scout *stateProto) {
//	_ = man.Write(func(state *stateProto) error {
//		state.MergeIterableCachedFields(scout)
//		return nil
//	})
//}
//
//// MergeIterableCachedFieldsInManager merges the stateProto managed by the given ScoutStateManager into the stateProto
//// managed by the referred to ScoutStateManager instance. This is done by first creating a Read scope for the passed in
//// ScoutStateManager, then creating a Write scope for the referred to ScoutStateManager, then finally merging the
//// CachedField(s).
//func (man *ScoutStateManager) MergeIterableCachedFieldsInManager(manager *ScoutStateManager) {
//	_ = manager.Read(func(stateToMerge *stateProto) error {
//		_ = man.Write(func(stateToMergeInto *stateProto) error {
//			stateToMergeInto.MergeIterableCachedFields(stateToMerge)
//			return nil
//		})
//		return nil
//	})
//}
//
//// GetCachedFieldRead calls stateProto.GetCachedField within a Read scope, then calls the given function with the
//// fetched CachedField within the same Read scope.
//func (man *ScoutStateManager) GetCachedFieldRead(fieldName string, fun func(state *stateProto, cachedField CachedField) error) error {
//	return man.Read(func(state *stateProto) error { return fun(man.state, state.GetCachedField(fieldName)) })
//}
//
//// GetCachedFieldWrite calls stateProto.GetCachedField within a Write scope, then calls the given function with the
//// fetched CachedField within the same Write scope.
//func (man *ScoutStateManager) GetCachedFieldWrite(fieldName string, fun func(state *stateProto, cachedField CachedField) error) error {
//	return man.Write(func(state *stateProto) error { return fun(man.state, state.GetCachedField(fieldName)) })
//}
//
//// CachedFieldGet calls stateProto.GetCachedField then CachedField.Get on the returned CachedField. Both of these are
//// executed within the same Read scope.
//func (man *ScoutStateManager) CachedFieldGet(fieldName string, key any) (val any, ok bool) {
//	_ = man.GetCachedFieldRead(fieldName, func(state *stateProto, cachedField CachedField) error {
//		val, ok = cachedField.Get(key)
//		return nil
//	})
//	return
//}
//
//// CachedFieldSetOrAdd calls stateProto.GetCachedField then CachedField.SetOrAdd on the returned CachedField. Both of
//// these are executed within the same Write scope.
//func (man *ScoutStateManager) CachedFieldSet(fieldName string, value any, components ...any) {
//	_ = man.GetCachedFieldWrite(fieldName, func(state *stateProto, cachedField CachedField) error {
//		cachedField.Set(value, components...)
//		return nil
//	})
//}
//
//// GetIterableCachedFieldRead calls stateProto.GetIterableCachedField within a Read scope, then calls the given function
//// with the fetched IterableCachedField within the same Read scope.
//func (man *ScoutStateManager) GetIterableCachedFieldRead(fieldName string, fun func(state *stateProto, field IterableCachedField) error) error {
//	return man.Read(func(state *stateProto) error { return fun(state, state.GetIterableCachedField(fieldName)) })
//}
//
//// GetIterableCachedFieldWrite calls stateProto.GetIterableCachedField within a Write scope, then calls the given
//// function with the fetched IterableCachedField within the same Write scope.
//func (man *ScoutStateManager) GetIterableCachedFieldWrite(fieldName string, fun func(state *stateProto, field IterableCachedField) error) error {
//	return man.Write(func(state *stateProto) error { return fun(state, state.GetIterableCachedField(fieldName)) })
//}
//
//// IterableCachedFieldLen calls stateProto.GetIterableCachedField then IterableCachedField.Len on the returned
//// IterableCachedField. Both of these are executed within the same Read scope.
//func (man *ScoutStateManager) IterableCachedFieldLen(fieldName string) (len int) {
//	_ = man.GetIterableCachedFieldRead(fieldName, func(state *stateProto, field IterableCachedField) error {
//		len = field.Len()
//		return nil
//	})
//	return
//}
//
//// IterableCachedFieldIter calls stateProto.GetIterableCachedField then IterableCachedField.Iter on the returned
//// IterableCachedField. Both of these are executed within the same Read scope.
//func (man *ScoutStateManager) IterableCachedFieldIter(fieldName string) (iter CachedFieldIterator) {
//	_ = man.GetIterableCachedFieldRead(fieldName, func(state *stateProto, field IterableCachedField) error {
//		iter = field.Iter()
//		return nil
//	})
//	return
//}
