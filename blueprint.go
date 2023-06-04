package state

import (
	"encoding/json"
	"fmt"
	"github.com/andygello555/gotils/v2/maps"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"io/fs"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	bpFilenameFmt     = "bp-%s"
	bpFilenameFmtJSON = bpFilenameFmt + string(JSON)
	bpFilenameFmtGob  = bpFilenameFmt + string(Gob)
	bpDefaultName     = ""
)

func funcName(f interface{}) []byte {
	return []byte(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name())
}

type BaseDirFunc func(state State) string

func DefaultBaseDir(state State) string {
	return state.(*stateProto).createdAt.Format(basePathFormat)
}

type IsBaseDirFunc func(info os.FileInfo) (time.Time, bool)

func DefaultIsBaseDir(info os.FileInfo) (time.Time, bool) {
	t, err := time.Parse(basePathFormat, info.Name())
	return t, err == nil
}

type StringFunc func(state State) string

func DefaultString(state State) string {
	reader, stateReadable := state.(StateReader)
	lines := make([]string, 0)
	fields := state.Blueprint().Fields()
	sort.Strings(fields)
	for _, name := range fields {
		// If the CachedField implements CachedFieldStringer or fmt.Stringer then we will just use the interface to
		// compute the String representation of the field.
		field := state.GetCachedField(name)
		if stringer, ok := field.(fmt.Stringer); ok {
			lines = append(lines, fmt.Sprintf("%s = %s", name, stringer.String()))
		} else if stringer, ok := field.(CachedFieldStringer); ok && stateReadable {
			lines = append(lines, fmt.Sprintf("%s = %s", name, stringer.String(reader)))
		} else {
			fieldValue := reflect.Indirect(reflect.ValueOf(field))
			fieldType := fieldValue.Type()
			switch fieldType.Kind() {
			case reflect.Struct:
				numFields := fieldType.NumField()
				for i := 0; i < numFields; i++ {
					fieldField := fieldValue.Field(i)
					if fieldType.Field(i).IsExported() {
						fieldFieldName := fieldType.Field(i).Name
						lines = append(lines, fmt.Sprintf("%s.%s = %v", name, fieldFieldName, fieldField.Interface()))
					}
				}
			default:
				lines = append(lines, fmt.Sprintf("%s = %v", name, fieldValue.Interface()))
			}
		}
	}
	return strings.Join(lines, "\n")
}

type NowFunc func() time.Time

func DefaultNow() time.Time {
	return time.Now().UTC()
}

type BlueprintFilenameFunc func(old string) string

func DefaultBlueprintFilename(old string) string {
	randomFilename := func() string {
		return fmt.Sprintf(bpFilenameFmtJSON, uuid.New().String())
	}
	var newFilename string
	for newFilename = randomFilename(); old == newFilename; newFilename = randomFilename() {
	}
	return newFilename
}

type IsBlueprintFilenameFunc func(filename string) bool

var defaultBlueprintFilenamePattern = regexp.MustCompile(`^bp-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}.json$`)

func DefaultIsBlueprintFilenameFunc(filename string) bool {
	return defaultBlueprintFilenamePattern.MatchString(filename)
}

type Blueprint interface {
	SetBaseDir(baseDir BaseDirFunc) Blueprint
	IsBaseDir(info os.FileInfo) (time.Time, bool)
	SetIsBaseDir(isBD IsBaseDirFunc) Blueprint
	SetString(string StringFunc) Blueprint
	Now() time.Time
	SetNow(now NowFunc) Blueprint
	SetBlueprintFilename(bpFnFunc BlueprintFilenameFunc) Blueprint
	IsBlueprintFilename(filename string) bool
	SetIsBlueprintFilename(isBPFn IsBlueprintFilenameFunc) Blueprint
	// Fields returns the names of all the fields within the Blueprint, except from the field holding the Blueprint
	// itself.
	Fields() []string
	json.Marshaler
	json.Unmarshaler
	CachedField
}

// Blueprint describes the internal structure of a State, as well as where it will be saved/loaded to/from.
type bpProto struct {
	emptyFields         map[string]CachedField
	filename            string
	name                string
	baseDirFunc         BaseDirFunc
	baseDirName         string
	isBDFunc            IsBaseDirFunc
	isBDName            string
	bothBDFuncAltered   bool
	stringFunc          StringFunc
	stringName          string
	nowFunc             NowFunc
	nowName             string
	bpFnFunc            BlueprintFilenameFunc
	bpFnName            string
	isBPFnFunc          IsBlueprintFilenameFunc
	isBPFnName          string
	bothBPFnFuncAltered bool
	computedNames       bool
}

func NewBlueprint(fields ...CachedField) Blueprint {
	bp := bpProto{
		emptyFields:         make(map[string]CachedField),
		filename:            DefaultBlueprintFilename(""),
		name:                bpDefaultName,
		baseDirFunc:         DefaultBaseDir,
		isBDFunc:            DefaultIsBaseDir,
		bothBDFuncAltered:   true,
		stringFunc:          DefaultString,
		nowFunc:             DefaultNow,
		bpFnFunc:            DefaultBlueprintFilename,
		isBPFnFunc:          DefaultIsBlueprintFilenameFunc,
		bothBPFnFuncAltered: true,
	}

	for _, field := range fields {
		// If a Blueprint has been embedded into a Blueprint, we will set the bpProto.name to be the name of that field
		// but not add it to the map
		name := field.Name()
		if _, ok := bp.emptyFields[name].(Blueprint); ok {
			bp.name = name
		} else {
			bp.emptyFields[name] = field
		}
	}
	bp.setFuncNames()
	return &bp
}

func (b *bpProto) SetBaseDir(baseDir BaseDirFunc) Blueprint {
	b.baseDirFunc = baseDir
	b.bothBDFuncAltered = !b.bothBDFuncAltered
	b.computedNames = false
	b.setFuncNames()
	return b
}

func (b *bpProto) IsBaseDir(info os.FileInfo) (time.Time, bool) { return b.isBDFunc(info) }

func (b *bpProto) SetIsBaseDir(parseBD IsBaseDirFunc) Blueprint {
	b.isBDFunc = parseBD
	b.bothBDFuncAltered = !b.bothBDFuncAltered
	b.computedNames = false
	b.setFuncNames()
	return b
}

func (b *bpProto) SetString(string StringFunc) Blueprint {
	b.stringFunc = string
	b.computedNames = false
	b.setFuncNames()
	return b
}

func (b *bpProto) Now() time.Time { return b.nowFunc() }

func (b *bpProto) SetNow(now NowFunc) Blueprint {
	b.nowFunc = now
	b.computedNames = false
	b.setFuncNames()
	return b
}

func (b *bpProto) SetBlueprintFilename(bpFnFunc BlueprintFilenameFunc) Blueprint {
	b.bpFnFunc = bpFnFunc
	b.filename = b.bpFnFunc(b.filename)
	b.bothBPFnFuncAltered = !b.bothBPFnFuncAltered
	b.computedNames = false
	b.setFuncNames()
	return b
}

func (b *bpProto) IsBlueprintFilename(filename string) bool { return b.isBPFnFunc(filename) }

func (b *bpProto) SetIsBlueprintFilename(isBPFnFunc IsBlueprintFilenameFunc) Blueprint {
	b.isBPFnFunc = isBPFnFunc
	b.bothBPFnFuncAltered = !b.bothBDFuncAltered
	b.computedNames = false
	b.setFuncNames()
	return b
}

func (b *bpProto) setFuncNames() {
	if !b.computedNames {
		for _, fn := range []struct {
			name *string
			fn   any
		}{
			{&b.baseDirName, b.baseDirFunc},
			{&b.isBDName, b.isBDFunc},
			{&b.stringName, b.stringFunc},
			{&b.nowName, b.nowFunc},
			{&b.bpFnName, b.bpFnFunc},
			{&b.isBPFnName, b.isBPFnFunc},
		} {
			if fn.fn != nil {
				*fn.name = string(funcName(fn.fn))
			}
		}
		b.computedNames = true
	}
}

type bpSerialisedRepresentation struct {
	NamesToTypes map[string]string `json:"names_to_types"`
	Name         string            `json:"name"`
	BaseDirName  string            `json:"base_dir_name"`
	IsBDName     string            `json:"is_bd_name"`
	StringName   string            `json:"string_name"`
	NowName      string            `json:"now_name"`
	BpFnName     string            `json:"bp_fn_name"`
	IsBPFnName   string            `json:"is_bp_fn_name"`
}

func bpSerialisedFrom(bp *bpProto) bpSerialisedRepresentation {
	bsr := bpSerialisedRepresentation{
		NamesToTypes: make(map[string]string),
		Name:         bp.name,
		BaseDirName:  bp.baseDirName,
		IsBDName:     bp.isBDName,
		StringName:   bp.stringName,
		NowName:      bp.nowName,
		BpFnName:     bp.bpFnName,
		IsBPFnName:   bp.isBPFnName,
	}
	maps.RangeKeys(bp.emptyFields, func(i int, key string, val CachedField) bool {
		bsr.NamesToTypes[key] = reflect.TypeOf(val).Name()
		return true
	})
	return bsr
}

func (bsr bpSerialisedRepresentation) matches(bp *bpProto) (err error) {
	if len(bsr.NamesToTypes) != len(bp.emptyFields) {
		return fmt.Errorf(
			"serialised representation of Blueprint contains %d CachedFields, whilst in-memory representation contains %d",
			len(bsr.NamesToTypes), len(bp.emptyFields),
		)
	}

	for name, fieldTypeName := range bsr.NamesToTypes {
		if cachedField, ok := bp.emptyFields[name]; !ok {
			return fmt.Errorf(
				"serialised representation of Blueprint CachedField %q, which does not exist in the in-memory representation",
				name,
			)
		} else if cachedFieldTypeName := reflect.TypeOf(cachedField).Name(); fieldTypeName != cachedFieldTypeName {
			return fmt.Errorf(
				"type of %q CachedField within serialised representation of Blueprint does not match the type of the %q CachedField in-memory (%q vs. %q)",
				name, name, fieldTypeName, cachedFieldTypeName,
			)
		}
	}

	for _, test := range []struct {
		desc string
		a    string
		b    string
	}{
		{"name of Blueprint CachedField", bsr.Name, bp.name},
		{"BaseDir function signature", bsr.BaseDirName, bp.baseDirName},
		{"IsBaseDir function signature", bsr.IsBDName, bp.isBDName},
		{"String function signature", bsr.StringName, bp.stringName},
		{"Now function signature", bsr.NowName, bp.nowName},
		{"BlueprintFilename function signature", bsr.BpFnName, bp.bpFnName},
		{"IsBlueprintFilename function signature", bsr.IsBPFnName, bp.isBPFnName},
	} {
		if test.a != test.b {
			return fmt.Errorf(
				"the %s of the serilised representation of the Blueprint does not match the in-memory representation (%q vs. %q)",
				test.desc, test.a, test.b,
			)
		}
	}
	return
}

func (b *bpProto) MarshalJSON() (data []byte, err error) {
	b.setFuncNames()
	bsr := bpSerialisedFrom(b)
	if data, err = json.Marshal(&bsr); err != nil {
		err = errors.Wrap(err, "cannot serialise Blueprint")
	}
	return
}

func (b *bpProto) UnmarshalJSON(data []byte) (err error) {
	var bsr bpSerialisedRepresentation
	if err = json.Unmarshal(data, &bsr); err != nil {
		err = errors.Wrapf(err, "cannot deserialise Blueprint")
		return
	}
	return bsr.matches(b)
}

func (b *bpProto) Fields() []string {
	return maps.OrderedKeys(b.emptyFields)
}

func (b *bpProto) make() map[string]CachedField {
	initialisedBlueprint := make(map[string]CachedField)
	for name, field := range b.emptyFields {
		if name != b.name {
			initialisedBlueprint[name] = field.Make()
		}
	}
	// Always set the Blueprint within the initialised Blueprint
	initialisedBlueprint[b.name] = b
	return initialisedBlueprint
}

func (b *bpProto) Name() string           { return b.name }
func (b *bpProto) Encoding() EncodingType { return JSON }

// Set writes the Blueprint in-memory to disk.
func (b *bpProto) Set(state StateWriter, value any, components ...any) (err error) {
	if len(components) > 0 {
		return errors.New("Blueprint is not addressable, it can only be loaded/saved in its entirety")
	}

	if !b.bothBDFuncAltered || !b.bothBPFnFuncAltered {
		return errors.New(
			"when setting BaseDir/BlueprintFilename, you must also set IsBaseDir/IsBlueprintFilename, and vice-versa",
		)
	}

	// We keep trying to add the bytes for the filename if there is a collision with an existing file
	dir := state.FSWriter()
	checkFilename := func() bool { return dir.Exists(b.filename) }
	for checkFilename() {
		b.filename = b.bpFnFunc(b.filename)
	}

	var data []byte
	if data, err = json.Marshal(&b); err != nil {
		return
	}
	return dir.WriteFile(b.filename, data, filePerms)
}

// Get needs to fetch the latest Blueprint from disk.
func (b *bpProto) Get(state StateReader, components ...any) (value any, err error) {
	if len(components) > 0 {
		return nil, errors.New("Blueprint is not addressable, it can only be loaded/saved in its entirety")
	}

	var bpFile fs.File
	dir := state.FSReader()
	if bpFile, err = dir.Open(b.filename); err != nil {
		return
	}

	err = json.NewDecoder(bpFile).Decode(&b)
	return nil, err
}

func (b *bpProto) Make() CachedField {
	return &bpProto{emptyFields: make(map[string]CachedField)}
}
