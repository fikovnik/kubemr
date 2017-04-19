package job

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/pkg/runtime"
	"k8s.io/client-go/pkg/watch"
)

// NewJobWatcher returns a function that watches all Backup TPRs in a given namespace
func NewJobWatcher(cl *dynamic.ResourceClient) func() (watch.Interface, error) {
	return func() (watch.Interface, error) {
		// watching for Jobs
		iface, err := cl.Watch(&MapReduceJob{})
		if err != nil {
			return nil, err
		}
		// run all watch events through a filter before returning them.
		// see the below watchFilterer function for info on what we're doing here
		return watch.Filter(iface, watchFilterer()), nil
	}
}

// this is a function that translates watch events for Backup TPRs so that consumers of
// the watch channel can cast objects in each event directly to *Backups
func watchFilterer() func(watch.Event) (watch.Event, bool) {
	return func(in watch.Event) (watch.Event, bool) {
		// event objects for TPRs come in as *runtime.Unstructured - a 'bucket' of unknown bytes
		// that Kubernetes converts to a map[string]interface{} for us
		unstruc, ok := in.Object.(*runtime.Unstructured)
		if !ok {
			log.Printf("Not an unstructured")
			return in, false
		}
		// marshal the map into JSON, then unmarshal it back into a *Backup so that we can return
		// it. If we fail anywhere, then indicate to the watch stream not to process this event.
		// the consumer of the watch stream won't care about the event.
		jb, err := UnstructToMapReduceJob(unstruc)
		if err != nil {
			return in, false
		}
		in.Object = jb
		return in, true
	}
}

//UnstructToMapReduceJob takes *runtime.Unstructured and marshals it into *MapReduceJob
func UnstructToMapReduceJob(in *runtime.Unstructured) (*MapReduceJob, error) {
	b, err := json.Marshal(in.Object)
	if err != nil {
		log.Printf("Error marshaling %#v (%s)", in.Object, err)
		return nil, err
	}
	jb := new(MapReduceJob)
	if err := json.Unmarshal(b, jb); err != nil {
		log.Printf("Error unmarshaling %s (%s)", string(b), err)
		return nil, err
	}
	return jb, nil
}
