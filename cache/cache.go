package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	corev1 "k8s.io/api/core/v1"
)

var mu sync.RWMutex
var totalCacheSize int64
var cache = make(map[string][]byte)

// or for concurrent access
// var cache sync.Map

func compressData(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func decompressData(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func GetEvent(key string) (*corev1.Event, error) {
	mu.RLock()
	defer mu.RUnlock()

	// Retrieve the compressed data from the cache
	compressedData, exists := cache[key]
	if !exists {
		return nil, errors.New("key not found")
	}

	// Decompress the data
	data, err := decompressData(compressedData)
	if err != nil {
		return nil, err
	}

	// Deserialize the data back into a CachedPod struct
	var event *corev1.Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}

	return event, nil
}

func SetEvent(key string, event *corev1.Event) error {
	mu.Lock()
	defer mu.Unlock()

	// Serialize the pod struct to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Compress the serialized data
	compressedData := compressData(data)

	cache[key] = compressedData

	updateCacheSize(int64(len(compressedData)))

	return nil
}

func RemoveEvent(key string) error {
	mu.Lock()
	defer mu.Unlock()

	compressedData, exists := cache[key]
	if !exists {
		return errors.New("key not found")
	}
	reduceCacheSize(int64(len(compressedData)))

	delete(cache, key)

	return nil
}

func updateCacheSize(size int64) {
	atomic.AddInt64(&totalCacheSize, size)
}

func reduceCacheSize(size int64) {
	atomic.AddInt64(&totalCacheSize, -size)
}

func getTotalCacheSize() int64 {
	return atomic.LoadInt64(&totalCacheSize)
}

func StartCacheMonitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			printCacheStats()
		}
	}()
}

func printCacheStats() {
	printFirstElement := false

	cacheSize := getTotalCacheSize()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Println("InMemory Cache Statistics:")
	fmt.Printf("  Number of Entries: %d\n", len(cache))
	fmt.Printf("  Total Cache Size (approx): %d mb\n", cacheSize/1024/1024)
	fmt.Printf("  Allocated Heap Objects: %d\n", memStats.HeapObjects)
	fmt.Printf("  Total Allocated Memory: %d mb\n", memStats.Alloc/1024/1024)
	fmt.Println("-------------------------------")

	if !printFirstElement {
		return
	}

	if len(cache) == 0 {
		return
	}

	// Retrieve an arbitrary element
	for k, _ := range cache {
		event, err := GetEvent(k)
		if err != nil {
			return
		}

		prettyJSON, err := json.MarshalIndent(event, "", "    ")
		if err != nil {
			fmt.Println("Error generating JSON:", err)
			return
		}

		fmt.Println("First Element in Cache:", k)
		fmt.Println(string(prettyJSON))
		break
	}

}
