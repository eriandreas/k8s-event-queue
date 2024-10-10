package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"sort"
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
	// Path to kubeconfig file
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// Build the client configuration from kubeconfig file
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	rxgo.Timer(rxgo.WithDuration(1 * time.Second)).DoOnNext(func(i interface{}) {
		fmt.Println("---- Timer called")
	})

	eventChan := make(chan rxgo.Item)

	// run informer to get events
	factory := informers.NewSharedInformerFactory(clientset, time.Minute*10)
	eventInformer := factory.Core().V1().Events().Informer()
	eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			eventChan <- rxgo.Of(obj)

			event := obj.(*corev1.Event)

			slice := max(0, min(len(event.Message)-1, 30))
			fmt.Printf("%d,%s: New event: %s\n", getMostRecentEventTimestamp(*event).UnixMilli(), createEventKey(*event), event.Message[:slice])
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			eventChan <- rxgo.Of(newObj)

			oldEvent := oldObj.(*corev1.Event)
			newEvent := newObj.(*corev1.Event)

			slice := max(0, min(len(newEvent.Message)-1, 30))
			fmt.Printf("%d,%s: Updated event: %s -> %s\n", getMostRecentEventTimestamp(*newEvent).UnixMilli(), createEventKey(*newEvent), oldEvent.Message, newEvent.Message[:slice])
		},
		DeleteFunc: func(obj interface{}) {
			// event := obj.(*corev1.Event)
			// slice := max(0, min(len(newEvent.Message) - 1, 30))
			// fmt.Printf("%d,%s: Deleted event: %s\n", getMostRecentEventTimestamp(*event).UnixMilli(), createEventKey(*event), event.Message[:slice])
		},
	})

	bufferDuration := rxgo.WithDuration(2 * time.Second)
	totalInterval := 1 * time.Second

	// Create Observable
	observable := rxgo.
		FromChannel(eventChan).
		BufferWithTime(bufferDuration).
		FlatMap(func(item rxgo.Item) rxgo.Observable {
			events := item.V.([]interface{})
			fmt.Println("----- Processing origin items len():", len(events))
			if len(events) == 0 {
				return rxgo.Empty()
			}

			// Group events
			groups := make(map[string][]corev1.Event)
			for _, event := range events {
				eventRef := event.(*corev1.Event)
				if eventRef == nil {
					continue
				}
				groupKey := groupingFunction(*eventRef)
				groups[groupKey] = append(groups[groupKey], *eventRef)
			}

			// Collect latest items from each group
			var latestItems []corev1.Event
			for _, group := range groups {
				// Sort group events
				sort.Slice(group, func(i, j int) bool {
					return sortingFunction(group[i], group[j])
				})
				// Get the latest item
				latestItem := group[len(group)-1]
				latestItems = append(latestItems, latestItem)
			}

			sort.Slice(latestItems, func(i, j int) bool {
				return sortingFunction(latestItems[i], latestItems[j])
			})

			numItems := len(latestItems)
			if numItems == 0 {
				return rxgo.Empty()
			}

			intervalDuration := totalInterval / time.Duration(numItems)

			fmt.Println("----- Processing reduced items len():", numItems, "with interval duration in millisec:", intervalDuration, "time window is always 1 second")

			// ctx := context.Background()
			// return rxgo.Interval(rxgo.WithDuration(interval), rxgo.WithContext(ctx)).Map(func(_ context.Context, i interface{}) (interface{}, error) {
			return rxgo.Interval(rxgo.WithDuration(intervalDuration)).Map(func(_ context.Context, i interface{}) (interface{}, error) {
				index := i.(int)
				if index >= numItems-1 {
					return nil, nil
				}

				return latestItems[index], nil
			}).TakeUntil(func(item interface{}) bool {
				return item == nil
			}).Filter(func(item interface{}) bool {
				return item != nil
			})
		})

	subscription := observable.Observe()

	go func() {
		// fmt.Println("subscription go routine called")
		for item := range subscription {
			// fmt.Println("Received item in subscription")

			if item.Error() {
				fmt.Println("Error:", item.E)
			} else {
				fmt.Println("")

				event := item.V.(corev1.Event)
				slice := max(0, min(len(event.Message)-1, 60))

				fmt.Println("--", createEventKey(event), ":", event.Message[:slice])
				// event := item.V.(corev1.Event)
				// processEvent(event)
			}
		}
	}()

	// Start the informer
	stopper := make(chan struct{})
	defer close(stopper)
	go eventInformer.Run(stopper)

	// Wait for the informer caches to synchronize
	if !cache.WaitForCacheSync(stopper, eventInformer.HasSynced) {
		panic("Failed to sync informer cache")
	}

	// Keep the main thread alive
	<-stopper
}

func groupingFunction(event corev1.Event) string {
	return createEventKey(event)
}

func sortingFunction(a, b corev1.Event) bool {
	return getMostRecentEventTimestamp(a).Before(getMostRecentEventTimestamp(b))
}

func processEvent(event corev1.Event) {
	// Your event processing logic

	fmt.Println("------- Processing event:", event)
}

func getMostRecentEventTimestamp(event corev1.Event) time.Time {
	// Collect all the timestamps into a slice
	timestamps := []time.Time{
		event.EventTime.Time,
		event.LastTimestamp.Time,
		event.FirstTimestamp.Time,
	}

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
