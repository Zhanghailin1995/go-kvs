package kvs

import (
	"fmt"
	"os"
	"testing"
)

func TestGetStoredValue(t *testing.T) {
	path := t.TempDir() + "/"
	defer os.RemoveAll(path)
	store, err := Open(path)
	if err != nil {
		t.Error(err)
		return
	}

	assertSetValue(t, store, "key1", "value1")
	assertSetValue(t, store, "key2", "value2")
	assertGetValue(t, store, "key1", "value1")
	assertGetValue(t, store, "key2", "value2")
	store.shutdown()

	store, err = Open(path)
	if err != nil {
		t.Error(err)
		return
	}
	defer store.shutdown()
	assertGetValue(t, store, "key1", "value1")
	assertGetValue(t, store, "key2", "value2")
}

func TestOverwriteValue(t *testing.T) {
	path := t.TempDir() + "/"
	defer os.RemoveAll(path)
	store, err := Open(path)
	if err != nil {
		t.Error(err)
		return
	}
	assertSetValue(t, store, "key1", "value1")
	assertGetValue(t, store, "key1", "value1")
	assertSetValue(t, store, "key1", "value2")
	assertGetValue(t, store, "key1", "value2")

	store.shutdown()

	store, err = Open(path)
	if err != nil {
		t.Error(err)
		return
	}
	defer store.shutdown()
	assertGetValue(t, store, "key1", "value2")
	assertSetValue(t, store, "key1", "value3")
	assertGetValue(t, store, "key1", "value3")
}

func TestGetNonExistentValue(t *testing.T) {
	path := t.TempDir() + "/"
	defer os.RemoveAll(path)
	store, err := Open(path)
	if err != nil {
		t.Error(err)
		return
	}
	assertSetValue(t, store, "key1", "value1")
	assertGetValue(t, store, "key2", "")

	store.shutdown()

	store, err = Open(path)
	if err != nil {
		t.Error(err)
		return
	}
	defer store.shutdown()
	assertGetValue(t, store, "key2", "")
}

func TestRemoveNonExistentKey(t *testing.T) {
	path := t.TempDir() + "/"
	defer os.RemoveAll(path)
	store, err := Open(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer store.shutdown()

	assertRemove(t, store, "key1")
}

func TestRemoveKey(t *testing.T) {
	path := t.TempDir() + "/"
	defer os.RemoveAll(path)
	store, err := Open(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer store.shutdown()

	assertSetValue(t, store, "key1", "value1")
	assertRemove(t, store, "key1")
	assertGetValue(t, store, "key1", "")
}

//func TestSetKey(t *testing.T) {
//	path := t.TempDir() + "/"
//	// path := "/home/hailin/temp/"
//	defer os.RemoveAll(path)
//	store, err := Open(path)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	for i := 0; i < 1000; i++ {
//		value := strconv.Itoa(i)
//		for keyId := 0; keyId < 1000; keyId++ {
//			key := fmt.Sprintf("key%d", keyId)
//			assertSetValue(t, store, key, value)
//		}
//	}
//	store.shutdown()
//	store, err = Open(path)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	for keyId := 0; keyId < 1000; keyId++ {
//		key := fmt.Sprintf("key%d", keyId)
//		assertGetValue(t, store, key, "999")
//	}
//
//}

//func TestCompaction(t *testing.T) {
//	path := t.TempDir() + "/"
//	// path := "/home/hailin/temp/"
//	defer os.RemoveAll(path)
//	store, err := Open(path)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//
//	dirSize := func() (int64, error) {
//		var size int64
//		err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
//			if !info.IsDir() {
//				size += info.Size()
//			}
//			return err
//		})
//		return size, err
//	}
//
//	currentSize, err := dirSize()
//	if err != nil {
//		t.Error(err)
//		return
//	}
//
//	for i := 0; i < 1000; i++ {
//		value := strconv.Itoa(i)
//		for keyId := 0; keyId < 1000; keyId++ {
//			key := fmt.Sprintf("key%d", keyId)
//			//fmt.Println(key)
//			assertSetValue(t, store, key, value)
//		}
//
//		newSize, err := dirSize()
//		if err != nil {
//			t.Error(err)
//			return
//		}
//
//		if newSize > currentSize {
//			currentSize = newSize
//			continue
//		}
//
//		store.shutdown()
//
//		store, err = Open(path)
//		if err != nil {
//			t.Error(err)
//			return
//		}
//		defer store.shutdown()
//		for keyId := 0; keyId < 1000; keyId++ {
//			key := fmt.Sprintf("key%d", keyId)
//			assertGetValue(t, store, key, value)
//		}
//		return
//	}
//	t.Error("no compaction detected ")
//}

func assertGetValue(t *testing.T, s *KvStore, k string, expect string) {
	v, err := s.Get(k)
	if err != nil {
		fmt.Printf("get key %s error\n", k)
		t.Error(err)
		return
	}
	assertEqual(t, v, expect)
}

func assertSetValue(t *testing.T, s *KvStore, k string, v string) {
	err := s.Set(k, v)
	if err != nil {
		t.Error(err)
	}
}

func assertRemove(t *testing.T, s *KvStore, k string) {
	err := s.Remove(k)
	if err != nil {
		t.Error(err)
	}
}

func assertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		t.Errorf("Not Equal [%v] [%v]", a, b)
	}
}
