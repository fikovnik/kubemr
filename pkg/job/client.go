package job

import (
	"encoding/json"

	log "github.com/Sirupsen/logrus"
	"github.com/turbobytes/kubemr/pkg/jsonpatch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/unversioned"
	"k8s.io/client-go/pkg/watch"
)

var (
	jobResource = &unversioned.APIResource{
		Name:       "mapreducejobs",
		Namespaced: true,
		Kind:       "MapReduceJobs",
	}
)

//Client manages all access to *Job types
type Client struct {
	tprclient *dynamic.Client
	jobclient *dynamic.ResourceClient
}

//NewClient creates a new client
func NewClient(tprclient *dynamic.Client) *Client {
	cl := &Client{tprclient: tprclient}
	//Blank namespace for --all-namespaces
	//Operations on individual Job needs their own namespaced ResourceClient
	cl.jobclient = cl.tprclient.Resource(jobResource, "")
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
				fallthrough
			case watch.Modified:
				switch jb.Status {
				case "":
					//Pre-flight checks, early abort
					err := cl.PatchJob(jb.Name, jb.Namespace, jb.initialize())
					if err != nil {
						log.Error(err)
					}
				case StatusPending:
					err := cl.Deploy(jb)
					if err != nil {
						log.Warn(err)
					}
					//TODO: Proceed with creating
				}
				//TODO: check/update progress/etc
			case watch.Deleted:
				//TODO: clear resources associated with it
			}
		}
	}
}

//Deploy allocates kubernetes objects
func (cl *Client) Deploy(jb *MapReduceJob) error {
	//Aquire a lock
	updateobj := jsonpatch.New()
	updateobj = updateobj.Add("test", "/status", StatusPending)
	updateobj = updateobj.Add("replace", "/status", StatusDeploying)
	err := cl.PatchJob(jb.Name, jb.Namespace, updateobj)
	if err != nil {
		//We failed to aquire the lock. Perhaps another operater is working on it
		return err
	}
	log.Infof("Aquired lock for %s/%s", jb.Namespace, jb.Name)
	//We now hold the lock. Fingers cross everything goes fine...
	if jb.Spec.spec.UserSecretName != "" {
		//TODO: Validate UserSecretName exists in ns, if provided

	}
	//TODO: Create kubemr secret in ns for this job
	//TODO: Create kubemr configmap in ns for this job
	//TODO: Create job object in ns
	//TODO: Update the job status
	return nil
}

//PatchJob applies an atomic json patch to the job
func (cl *Client) PatchJob(name, ns string, patchObj jsonpatch.Patch) error {
	//Need to make a new ResourceClient based on the ns of the object
	log.Info(patchObj)
	rescl := cl.tprclient.Resource(jobResource, ns)
	b, err := json.Marshal(patchObj)
	if err != nil {
		return err
	}
	res, err := rescl.Patch(name, api.JSONPatchType, b)
	if err != nil {
		log.Info(res)
		return err
	}
	return nil
}
