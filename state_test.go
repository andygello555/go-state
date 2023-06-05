package state

import (
	"flag"
	"fmt"
	"github.com/andygello555/gotils/v2/numbers"
	"github.com/andygello555/gotils/v2/slices"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

// This example shows the basics on how to use the go-state module:
//  1. It first initialises a Blueprint for our State instance
//  2. Then it creates a new State from the Blueprint by using the LoadNewest function
//  3. It then shows how to add values to the CachedField(s) of the State
//  4. We then showcase how the State can be saved to disk then retrieved in a new State instance by using LoadNewest
//  5. Finally, we show how to fetch values from a CachedField within the loaded State
func Example() {
	// Create the Blueprint for our State instances:
	//
	// Blueprint's store the information on where to save a State to disk. State's are saved to timestamped directories,
	// the time format can be customised by passing implementations to Blueprint.SetBaseDir and Blueprint.SetIsBaseDir.
	// This is useful when needing to continue from the latest saved State.
	const customBaseDirFormat = "people-02-01-2006T15-04"
	blueprint := NewBlueprint(
		Map[string, string]("name_to_address", JSON),
		Map[string, time.Time]("name_to_birthday", JSON),
		Map[int, string]("person_priority", JSON),
	).SetBaseDir(func(state State) string {
		return state.CreatedAt().Format(customBaseDirFormat)
	}).SetIsBaseDir(func(info os.FileInfo) (time.Time, bool) {
		t, err := time.Parse(customBaseDirFormat, info.Name())
		return t, err == nil
	})
	state := New(blueprint)

	fmt.Println("Empty state:")
	fmt.Println(state)

	state.MustSet("name_to_address", "13 Nelson St", "John S")
	state.MustSet("name_to_address", "93 Heath Rd", "Alice K")

	state.MustSet(
		"name_to_birthday",
		time.Date(1972, 1, 27, 0, 0, 0, 0, time.UTC), "John S",
	)
	state.MustSet(
		"name_to_birthday",
		time.Date(2000, 6, 26, 0, 0, 0, 0, time.UTC), "Alice K",
	)

	state.MustSet("person_priority", "Alice K", 1)
	state.MustSet("person_priority", "John S", 2)

	fmt.Println()
	fmt.Println("Filled state:")
	fmt.Println(state)

	state = nil

	var err error
	if state, err = LoadNewest(blueprint); err != nil {
		fmt.Printf("Could not load State from Blueprint: %v\n", err)
	}
	defer state.Delete()

	fmt.Println()
	fmt.Println("Loaded state:")
	fmt.Println(state)

	fmt.Println()
	const aliceKey = "Alice K"
	aliceAddress := state.MustGet("name_to_address", aliceKey)
	fmt.Printf("The address of %s is %s\n", aliceKey, aliceAddress)
	// Output:
	// Empty state:
	// name_to_address = map[]
	// name_to_birthday = map[]
	// person_priority = map[]
	//
	// Filled state:
	// name_to_address = map[Alice K:93 Heath Rd, John S:13 Nelson St]
	// name_to_birthday = map[Alice K:2000-06-26 00:00:00 +0000 UTC, John S:1972-01-27 00:00:00 +0000 UTC]
	// person_priority = map[1:Alice K, 2:John S]
	//
	// Loaded state:
	// name_to_address = map[Alice K:93 Heath Rd, John S:13 Nelson St]
	// name_to_birthday = map[Alice K:2000-06-26 00:00:00 +0000 UTC, John S:1972-01-27 00:00:00 +0000 UTC]
	// person_priority = map[1:Alice K, 2:John S]
	//
	// The address of Alice K is 93 Heath Rd
}

type mapTestCase[K comparable] struct {
	desc    string
	keys    []K
	values  []string
	ordered bool
	enc     EncodingType
}

func testMap[K comparable](t *testing.T, mtc mapTestCase[K]) {
	t.Helper()
	sig := fmt.Sprintf("map[%T]string%s", *new(K), mtc.enc.Ext())

	bp := NewBlueprint(
		Map[K, string](sig, mtc.enc),
	).SetBaseDir(func(state State) string {
		return sig
	}).SetIsBaseDir(func(info os.FileInfo) (time.Time, bool) {
		return time.Time{}, info.Name() == sig
	})
	state := New(bp)
	defer state.Delete()

	//bpMergeFrom := NewBlueprint(
	//	Map[K, string](sig, mtc.enc),
	//).SetBaseDir(func(state State) string {
	//	return sig + ".mergeFrom"
	//}).SetIsBaseDir(func(info os.FileInfo) (time.Time, bool) {
	//	return time.Time{}, info.Name() == sig + ".mergeFrom"
	//})
	//stateMergeFrom := New(bpMergeFrom)
	//defer stateMergeFrom.Delete()

	t.Run(fmt.Sprintf("%s - %s", sig, mtc.desc), func(t *testing.T) {
		set := make(map[K]string)
		for keyNo, key := range mtc.keys {
			value := mtc.values[keyNo]
			set[key] = value
			if err := state.Set(sig, value, key); err != nil {
				t.Errorf("error occurred whilst setting value %q for key %v in %s: %v", value, key, sig, err)
			}
		}

		shuffledKeys := make([]K, len(mtc.keys))
		copy(shuffledKeys, mtc.keys)
		rand.Shuffle(len(shuffledKeys), func(i, j int) {
			shuffledKeys[i], shuffledKeys[j] = shuffledKeys[j], shuffledKeys[i]
		})

		for _, key := range shuffledKeys {
			if value, err := state.Get(sig, key); err != nil {
				t.Errorf("error occurred whilst getting value for key %v in %s: %v", key, sig, err)
			} else if value != set[key] {
				t.Errorf("value retrieved from key %v is %q and not %q", key, value, set[key])
			}
		}

		iter, err := state.Iter(sig)
		if err != nil {
			t.Errorf("error occurred whilst creating iterator for %s: %v", sig, err)
		}

		var b strings.Builder
		b.WriteString("map[")
		for iter.Continue() {
			var (
				key   K
				value any
			)

			ordinal := numbers.Ordinal(iter.I() + 1)
			key = iter.Components()[0].(K)
			if value, err = iter.Get(); err != nil {
				t.Errorf("error occurred whilst retrieving value for %s element in an iterator for %s: %v", ordinal, sig, err)
			}
			b.WriteString(fmt.Sprintf("%v:%v", key, value))

			if mtc.keys[iter.I()] != key {
				t.Errorf("%s key is not %v it is %v", ordinal, mtc.keys[iter.I()], key)
			}

			if mtc.values[iter.I()] != value.(string) {
				t.Errorf("%s value is not %q it is %q", ordinal, mtc.values[iter.I()], value.(string))
			}

			if iter.I() < iter.Len()-1 {
				b.WriteString(", ")
			}

			if err = iter.Next(); err != nil {
				t.Errorf("error occurred whilst fetch next element (no. %d) from iterator for %s: %v", iter.I(), sig, err)
			}
		}
		b.WriteString("]")
		t.Logf("unordered %s: %s", sig, b.String())

		str := state.GetCachedField(sig).(CachedFieldStringer).String(state.(StateReader))
		if mtc.ordered && str != b.String() {
			t.Errorf("keys are ordered but string constructed from iterator does not much string constructed by CachedFieldStringer: %q vs %q", b.String(), str)
		}

		t.Logf("ordered %s: %s", sig, str)
		pairs := strings.Split(strings.TrimSuffix(strings.TrimPrefix(str, "map["), "]"), ", ")
		slices.Order(mtc.keys)

		for i, pair := range pairs {
			keyVal := strings.Split(pair, ":")
			if key := fmt.Sprintf("%v", mtc.keys[i]); key != keyVal[0] {
				t.Errorf("key for pair no. %d (%v) in string repr. for %s is %q not %q", i, pair, sig, keyVal[0], key)
			}
			if value := set[mtc.keys[i]]; value != keyVal[1] {
				t.Errorf("value for pair no. %d (%v) in string repr. for %s is %q not %q", i, pair, sig, keyVal[1], value)
			}
		}
	})
}

func generateMapCases[K comparable](no int, generate func(no int) []K) []mapTestCase[K] {
	toString := func(idx int, value K, arr []K) string {
		return fmt.Sprintf("%v", value)
	}

	cases := make([]mapTestCase[K], 0)
	for _, enc := range []EncodingType{JSON} {
		ordered := generate(no)
		orderedValues := slices.Comprehension(ordered, toString)

		shuffled := make([]K, len(ordered))
		copy(shuffled, ordered)
		shuffledValues := make([]string, len(orderedValues))
		copy(shuffledValues, orderedValues)

		rand.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			shuffledValues[i], shuffledValues[j] = shuffledValues[j], shuffledValues[i]
		})

		const (
			probInc = 0.05
			maxProb = 1.0
			maxGap  = 10
		)

		prob := 0.0
		gap := 0
		idx := make([]int, 0)
		for i := 0; i < len(ordered); i++ {
			if gap > maxGap {
				prob = 0.0
			}

			if rand.Float64() < prob {
				idx = append(idx, i)
				gap++
			} else {
				prob = numbers.Clamp(prob+probInc, maxProb)
				gap = 0
			}

			if len(idx) >= no/3 {
				break
			}
		}

		sparse := slices.RemoveElems(ordered, idx...)
		sparseValues := slices.RemoveElems(orderedValues, idx...)

		var zero K
		zeroSize := no / 20
		rest := generate(zeroSize)
		zeroStart := make([]K, 1, zeroSize+1)
		zeroStart[0] = zero
		zeroStart = append(zeroStart, rest...)
		zeroStartValues := slices.Comprehension(zeroStart, toString)

		zeroSecond := make([]K, 2, zeroSize+1)
		zeroSecond[0] = rest[0]
		zeroSecond[1] = zero
		zeroSecond = append(zeroSecond, rest[1:]...)
		zeroSecondValues := slices.Comprehension(zeroSecond, toString)

		zeroMiddle := slices.AddElems(rest, []K{zero}, zeroSize/2)
		zeroMiddleValues := slices.Comprehension(zeroMiddle, toString)

		zeroEnd := make([]K, 0, zeroSize+1)
		zeroEnd = append(zeroEnd, rest...)
		zeroEnd = append(zeroEnd, zero)
		zeroEndValues := slices.Comprehension(zeroEnd, toString)

		cases = append(cases,
			mapTestCase[K]{fmt.Sprintf("%d-ordered-%s", no, enc), ordered, orderedValues, true, enc},
			mapTestCase[K]{fmt.Sprintf("%d-shuffled-%s", no, enc), shuffled, shuffledValues, false, enc},
			mapTestCase[K]{fmt.Sprintf("%d-sparse-%s", len(sparse), enc), sparse, sparseValues, true, enc},
			mapTestCase[K]{fmt.Sprintf("zero-key-(%v)-at-start-of-%d-elems", zero, zeroSize), zeroStart, zeroStartValues, true, enc},
			mapTestCase[K]{fmt.Sprintf("zero-key-(%v)-second-of-%d-elems", zero, zeroSize), zeroSecond, zeroSecondValues, false, enc},
			mapTestCase[K]{fmt.Sprintf("zero-key-(%v)-in-middle-of-%d-elems", zero, zeroSize), zeroMiddle, zeroMiddleValues, false, enc},
			mapTestCase[K]{fmt.Sprintf("zero-key-(%v)-at-end-of-%d-elems", zero, zeroSize), zeroEnd, zeroEndValues, false, enc},
		)
	}
	return cases
}

var maxKeys = flag.Int("maxKeys", 100, "the maximum number of keys that will be generated for TestMap")

func TestMap(t *testing.T) {
	pad := fmt.Sprintf("%%0%dd", int(math.Floor(math.Log10(float64(*maxKeys))+1.0)))
	for _, c := range generateMapCases(*maxKeys, func(no int) []string {
		return slices.Comprehension(numbers.Range(0, no-1, 1), func(idx int, value int, arr []int) string {
			return fmt.Sprintf(pad, value)
		})
	}) {
		testMap(t, c)
	}

	for _, c := range generateMapCases(*maxKeys, func(no int) []int {
		return numbers.Range(1, no, 1)
	}) {
		testMap(t, c)
	}

	for _, c := range generateMapCases(*maxKeys, func(no int) []float64 {
		return numbers.Range(1.0, float64(no), 1.0)
	}) {
		testMap(t, c)
	}

	type structure struct {
		X int
		Y float64
		Z string
	}

	for _, c := range generateMapCases(*maxKeys, func(no int) []structure {
		return slices.Comprehension(numbers.Range(0, no-1, 1), func(idx int, value int, arr []int) structure {
			return structure{
				X: value,
				Y: float64(value + 1),
				Z: fmt.Sprintf(pad, value+2),
			}
		})
	}) {
		testMap(t, c)
	}

	for _, c := range generateMapCases(*maxKeys, func(no int) [][3]int {
		return slices.Comprehension(numbers.Range(0, no-1, 1), func(idx int, value int, arr []int) [3]int {
			return [3]int{value, value + 1, value + 2}
		})
	}) {
		testMap(t, c)
	}
}
