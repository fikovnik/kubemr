package job

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/pkg/api/unversioned"
	"k8s.io/client-go/pkg/watch"
)

//Client manages all access to *Job types
type Client struct {
	tprclient *dynamic.Client
	jobclient *dynamic.ResourceClient
}

//NewClient creates a new client
func NewClient(tprclient *dynamic.Client) *Client {
	cl := &Client{tprclient: tprclient}
	resource := &unversioned.APIResource{
		Name:       "mapreducejobs",
		Namespaced: true,
		Kind:       "MapReduceJobs",
	}
	cl.jobclient = cl.tprclient.Resource(resource, "default")
	return cl
}

//Get a MapReduceJob by name
func (cl *Client) Get(jobname, namespace string) MapReduceJob {
	return MapReduceJob{} //TODO
}

//List all jobs
func (cl *Client) List() (MapReduceJobList, error) {
	jobList := MapReduceJobList{}
	in, err := cl.jobclient.List(nil)
	if err != nil {
		log.Info(err)
		return jobList, err
	}
	b, err := json.Marshal(in)
	if err != nil {
		return jobList, err
	}
	err = json.Unmarshal(b, &jobList)
	return jobList, err //TODO
}

//Watch monitors single job by name
func (cl *Client) Watch(jobname, namespace string) {
	//TODO
}

//WatchList monitors all jobs
func (cl *Client) WatchList() error {
	openWatcher := NewJobWatcher(cl.jobclient)
	for {
		watcher, err := openWatcher()
		if err != nil {
			return err
		}
		defer watcher.Stop()
		watchCh := watcher.ResultChan()
		for evt := range watchCh {
			log.Printf("received event %#v", evt)
			jb, ok := evt.Object.(*MapReduceJob)
			if !ok {
				log.Printf("event was not a *MapReduceJob, skipping")
				continue
			}
			log.Printf("Job %s, status %s", jb.Name, jb.Status)
			switch evt.Type {
			case watch.Added:
				//TODO: make fail or pending
			case watch.Deleted:
				//TODO: clear resources associated with it
			}
		}
	}
}
