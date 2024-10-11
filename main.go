package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	rxgo "github.com/reactivex/rxgo/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	kubeconfig := getKubeConfigPath()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	// Set up event channel and informer
	eventChan := make(chan rxgo.Item)
	setupInformer(clientset, eventChan)

	// Create and process observables
	processEventObservables(eventChan)

	// Keep the main thread alive
	stopper := make(chan struct{})
	defer close(stopper)
	<-stopper
}

func getKubeConfigPath() *string {
	if home := homedir.HomeDir(); home != "" {
		return flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	}
	return flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
}

func setupInformer(clientset *kubernetes.Clientset, eventChan chan rxgo.Item) {
	factory := informers.NewSharedInformerFactory(clientset, time.Minute*10)
	eventInformer := factory.Core().V1().Events().Informer()

	eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { handleAddEvent(obj, eventChan) },
		UpdateFunc: func(oldObj, newObj interface{}) { handleUpdateEvent(oldObj, newObj, eventChan) },
		DeleteFunc: func(obj interface{}) { /* Optionally handle delete events here */ },
	})

	go eventInformer.Run(make(chan struct{}))

	// Wait for the informer caches to synchronize
	if !cache.WaitForCacheSync(make(chan struct{}), eventInformer.HasSynced) {
		panic("Failed to sync informer cache")
	}
}

func handleAddEvent(obj interface{}, eventChan chan rxgo.Item) {
	eventChan <- rxgo.Of(obj)
	printEventDetails("New", obj.(*corev1.Event))
}

func handleUpdateEvent(oldObj, newObj interface{}, eventChan chan rxgo.Item) {
	eventChan <- rxgo.Of(newObj)
	printEventDetails("Updated", newObj.(*corev1.Event), oldObj.(*corev1.Event))
}

func printEventDetails(eventType string, newEvent *corev1.Event, oldEvent ...*corev1.Event) {
	message := fmt.Sprintf("%d,%s: %s event: %s", getMostRecentEventTimestamp(*newEvent).UnixMilli(), createEventKey(*newEvent), eventType, newEvent.Message[:min(30, len(newEvent.Message))])
	if len(oldEvent) > 0 {
		message = fmt.Sprintf("%s -> %s", oldEvent[0].Message, newEvent.Message[:min(30, len(newEvent.Message))])
	}
	fmt.Println(message)
}

func processEventObservables(eventChan chan rxgo.Item) {
	bufferDuration := rxgo.WithDuration(2 * time.Second)
	totalInterval := 1 * time.Second

	observable := rxgo.FromChannel(eventChan).
		BufferWithTime(bufferDuration).
		FlatMap(func(item rxgo.Item) rxgo.Observable {
			return createGroupedObservable(item, totalInterval)
		})

	subscription := observable.Observe()

	go func() {
		for item := range subscription {
			if item.Error() {
				fmt.Println("Error:", item.E)
			} else {
				processEvent(item.V.(corev1.Event))
			}
		}
	}()
}

func createGroupedObservable(item rxgo.Item, totalInterval time.Duration) rxgo.Observable {
	events := item.V.([]interface{})
	if len(events) == 0 {
		return rxgo.Empty()
	}

	groups := groupEvents(events)

	var latestItems []corev1.Event
	for _, group := range groups {
		sort.Slice(group, sortingFunctionWrapper(group))
		latestItems = append(latestItems, group[len(group)-1])
	}

	numItems := len(latestItems)
	if numItems == 0 {
		return rxgo.Empty()
	}

	intervalDuration := totalInterval / time.Duration(numItems+1)
	fmt.Println("")
	fmt.Printf("----- Processing reduced items len(): %d of %d with interval duration: %v\n", numItems, len(events), intervalDuration)
	fmt.Println("")

	return rxgo.Create([]rxgo.Producer{func(ctx context.Context, chOut chan<- rxgo.Item) {
		intervalTicker := time.NewTicker(intervalDuration)
		defer intervalTicker.Stop()

		for i := 0; i < numItems; i++ {
			select {
			case <-ctx.Done():
				return
			case <-intervalTicker.C:
				chOut <- rxgo.Of(latestItems[i])
			}
		}
	}})
}

func groupEvents(events []interface{}) map[string][]corev1.Event {
	groups := make(map[string][]corev1.Event)
	for _, event := range events {
		eventRef := event.(*corev1.Event)
		if eventRef != nil {
			groupKey := createEventKey(*eventRef)
			groups[groupKey] = append(groups[groupKey], *eventRef)
		}
	}
	return groups
}

func sortingFunctionWrapper(group []corev1.Event) func(i, j int) bool {
	return func(i, j int) bool {
		return sortingFunction(group[i], group[j])
	}
}

func sortingFunction(a, b corev1.Event) bool {
	// First, compare based on the most recent timestamp
	timeA, timeB := getMostRecentEventTimestamp(a), getMostRecentEventTimestamp(b)

	if !timeA.Equal(timeB) {
		return timeA.Before(timeB)
	}

	// If timestamps are equal, compare based on the version
	versionAInt, errA := strconv.Atoi(a.ResourceVersion)
	versionBInt, errB := strconv.Atoi(b.ResourceVersion)

	if errA == nil && errB == nil {
		return versionAInt < versionBInt
	}

	return a.ResourceVersion < b.ResourceVersion
}

func processEvent(event corev1.Event) {
	fmt.Printf("-- %d %s : %s\n", getMostRecentEventTimestamp(event).UnixMilli(), createEventKey(event), event.Message[:min(60, len(event.Message))])
}

func getMostRecentEventTimestamp(event corev1.Event) time.Time {
	timestamps := []time.Time{event.EventTime.Time, event.LastTimestamp.Time, event.FirstTimestamp.Time}
	latest := time.Time{}
	for _, t := range timestamps {
		if !t.IsZero() && t.After(latest) {
			latest = t
		}
	}
	return latest
}

func createEventKey(event corev1.Event) string {
	return fmt.Sprintf("%s/%s/%s", event.InvolvedObject.Kind, event.InvolvedObject.Namespace, event.InvolvedObject.Name)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
