package sparkey

//#cgo LDFLAGS: -lsparkey
//#include <stdlib.h>
//#include <sparkey/sparkey.h>
import "C"
import "unsafe"

/* Log iterator */

// LogIter is a sequential log iterator.
// Iterators are not threadsafe, do not share them
// across multiple goroutines.
//
//  Example usage:
//
//     reader, _  := OpenLogReader("test.spl")
//     iter, _ := reader.Iterator()
//     for iter.Next(); iter.Valid(); iter.Next() {
//	       key, _ := iter.Key()
//	       val, _ := iter.Value()
//         fmt.Println("K/V", key, value)
//     }
//     if err := iter.Err(); err != nil {
//         fmt.Println("ERROR", err.Error())
//     }
//
type LogIter struct {
	iter *C.sparkey_logiter
	log  *C.sparkey_logreader
	err  error
}

// Err returns an error if one has occurred during iteration.
func (i *LogIter) Err() error {
	return i.err
}

// Closes a log iterator.
// This is a failsafe operation.
func (i *LogIter) Close() {
	if i.iter != nil {
		C.sparkey_logiter_close(&i.iter)
	}
	i.iter = nil
}

// Skip skips a number of entries.
// This is equivalent to calling Next count number of times.
func (i *LogIter) Skip(count int) error {
	rc := C.sparkey_logiter_skip(i.iter, i.log, C.int(count))
	if rc != rc_SUCCESS && rc != rc_ITERINACTIVE-205 {
		i.err = Error(rc)
	}
	return errorOrNil(rc)
}

// Next prepares the iterator to start reading from the next entry.
// The value of State() will be:
//   ITERATOR_CLOSED if the last entry has been passed.
//   ITERATOR_INVALID if anything goes wrong.
//   ITERATOR_ACTIVE if it successfully reached the next entry.
func (i *LogIter) Next() error {
	rc := C.sparkey_logiter_next(i.iter, i.log)
	if rc != rc_SUCCESS && rc != rc_ITERINACTIVE {
		i.err = Error(rc)
	}
	return errorOrNil(rc)
}

// Reset resets the iterator to the start of the current entry. This is only valid if
// state is ITERATOR_ACTIVE.
func (i *LogIter) Reset() error {
	rc := C.sparkey_logiter_reset(i.iter, i.log)
	return errorOrNil(rc)
}

// Valid returns true if iterator is at a valid position
func (i *LogIter) Valid() bool {
	return i.State() == ITERATOR_ACTIVE
}

// State gets the state for an iterator.
func (i *LogIter) State() IteratorState {
	return IteratorState(C.sparkey_logiter_state(i.iter))
}

// EntryType returns the type of the current entry.
func (i *LogIter) EntryType() EntryType {
	return EntryType(C.sparkey_logiter_type(i.iter))
}

// KeyLen returns the key length of the current entry.
func (i *LogIter) KeyLen() uint64 {
	return uint64(C.sparkey_logiter_keylen(i.iter))
}

// ValueLen returns the value length of the current entry.
func (i *LogIter) ValueLen() uint64 {
	return uint64(C.sparkey_logiter_valuelen(i.iter))
}

// Key returns the full key at the current position.
// This method will return a result only once per iteration.
func (i *LogIter) Key() ([]byte, error) {
	return i.KeyChunk(maxInt)
}

// KeyChunk returns a chunk of the key at the current position.
func (i *LogIter) KeyChunk(maxlen int) ([]byte, error) {
	var cLen C.uint64_t
	var cPtr *C.uint8_t

	rc := C.sparkey_logiter_keychunk(i.iter, i.log, C.uint64_t(maxlen), &cPtr, &cLen)
	if rc != rc_SUCCESS {
		return nil, Error(rc)
	}
	return C.GoBytes(unsafe.Pointer(cPtr), C.int(cLen)), nil
}

// Value returns the full values at the current position.
// This method will return a result only once per iteration.
func (i *LogIter) Value() ([]byte, error) {
	return i.ValueChunk(maxInt)
}

// ValueChunk returns a chunk of the value at the current position.
func (i *LogIter) ValueChunk(maxlen int) ([]byte, error) {
	var size C.uint64_t
	var ptr *C.uint8_t

	rc := C.sparkey_logiter_valuechunk(i.iter, i.log, C.uint64_t(maxlen), &ptr, &size)
	if rc != rc_SUCCESS {
		return nil, Error(rc)
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(size)), nil
}

// Compare compares the keys of two iterators pointing to the same log.
// It assumes that the iterators are both clean, i.e. nothing has been consumed from the current entry.
// It will return zero if the keys are equal, negative if key1 is smaller than key2 and positive if key1 is larger than key2.
func (i *LogIter) Compare(other *LogIter) (int, error) {
	var res C.int
	rc := C.sparkey_logiter_keycmp(i.iter, other.iter, i.log, &res)
	if rc != rc_SUCCESS {
		return 0, Error(rc)
	}
	return int(res), nil
}

/* Hash iterator */

// A hash iterator is an extension to the log iterator and implements
// additional methods for iteration and retrieval
type HashIter struct {
	*LogIter
	reader *HashReader
}

// Seek positions the cursor on the given key.
// Sets the iterator state to ITERATOR_INVALID when key cannot be found.
func (i *HashIter) Seek(key []byte) error {
	var k *C.uint8_t

	lk := len(key)
	if lk > 0 {
		k = (*C.uint8_t)(&key[0])
	}
	rc := C.sparkey_hash_get(i.reader.hash, k, C.uint64_t(lk), i.iter)
	return errorOrNil(rc)
}

// Get retrieves a value for a given key
// Returns nil when a value cannot be found.
func (i *HashIter) Get(key []byte) ([]byte, error) {
	if err := i.Seek(key); err != nil {
		return nil, err
	} else if i.State() == ITERATOR_ACTIVE {
		return i.Value()
	}
	return nil, nil
}

// NextLive positions the cursor at the next non-deleted "live" key
func (i *HashIter) NextLive() error {
	rc := C.sparkey_logiter_hashnext(i.iter, i.reader.hash)
	if rc != rc_SUCCESS && rc != rc_ITERINACTIVE {
		i.err = Error(rc)
	}
	return errorOrNil(rc)
}
