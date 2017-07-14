package worker

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"

	log "github.com/sirupsen/logrus"
	"github.com/turbobytes/kubemr/pkg/job"
	"github.com/turbobytes/kubemr/pkg/jsonpatch"
	"github.com/turbobytes/kubemr/pkg/k8s"
)

//Runner manages the lifecycle of a worker
type Runner struct {
	job           *job.MapReduceJob
	args, secrets map[string]string //Job specific things
	jobArgs       map[string]string //Job specific things
	cl            *job.Client
	name, ns      string
	hostname      string //for locking, debugging
	utils         *Utilities
}

//NewRunner initializes things from enviornment and returns a NewRunner
func NewRunner(apiserver, kubeconfig string) (*Runner, error) {
	k8sconfig, err := k8s.GetConfig(apiserver, kubeconfig)
	if err != nil {
		return nil, err
	}
	cfg := &job.Config{
		S3Region:     os.Getenv("KUBEMR_S3_REGION"),
		S3Endpoint:   os.Getenv("KUBEMR_S3_ENDPOINT"),
		BucketName:   os.Getenv("KUBEMR_S3_BUCKET_NAME"),
		BucketPrefix: os.Getenv("KUBEMR_S3_BUCKET_PREFIX"),
	}
	//Initialize utils
	auth := aws.Auth{
		AccessKey: os.Getenv("KUBEMR_S3_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("KUBEMR_S3_SECRET_ACCESS_KEY"),
	}
	region, ok := aws.Regions[cfg.S3Region]
	if !ok {
		return nil, fmt.Errorf("Unable to load S3 region")
	}
	if cfg.S3Endpoint != "" {
		region.S3Endpoint = cfg.S3Endpoint
	}
	s := s3.New(auth, region)

	jobclient, err := job.NewClient(k8sconfig, cfg)
	if err != nil {
		return nil, err
	}
	r := &Runner{}
	r.hostname, err = os.Hostname()
	if err != nil {
		return nil, err
	}
	r.cl = jobclient
	r.jobArgs = cfg.Map()
	//Store the name, namespace
	r.name = os.Getenv("KUBEMR_JOB_NAME")
	r.ns = os.Getenv("KUBEMR_JOB_NAMESPACE")
	//Load user secret if provided
	userSecretName := os.Getenv("KUBEMR_USER_SECRET")
	if userSecretName != "" {
		r.secrets, err = r.cl.GetSecretData(r.name, r.ns)
		if err != nil {
			return nil, err
		}
	}
	//Load the job
	r.job, err = r.cl.Get(r.name, r.ns)
	if err != nil {
		return nil, err
	}
	switch r.job.Status {
	case "":
		return nil, fmt.Errorf("Uninitialized job")
	case job.StatusFail:
		fallthrough
	case job.StatusPending:
		fallthrough
	case job.StatusDeploying:
		//Wait few secs and retry. Maybe we need to limit number of retries
		time.Sleep(time.Second * 10)
		return NewRunner(apiserver, kubeconfig)
	case job.StatusComplete:
		return nil, fmt.Errorf(r.job.Status)
	}
	prefix := os.Getenv("KUBEMR_S3_BUCKET_PREFIX") + "/" + r.job.Name + "/"

	//Ensure bucket exists
	bucket := s.Bucket(os.Getenv("KUBEMR_S3_BUCKET_NAME"))
	//TODO: Check before attempting put. Maybe this should be done elsewhere
	bucket.PutBucket("")
	r.args = r.job.GetJobArgs()
	r.utils = NewUtilities(bucket, prefix)
	//TODO
	return r, nil
}

//Run runs a worker
func (r *Runner) Run(w JobWorker) error {
	for {
		err := r.work(w)
		if err != nil {
			return err
		}
		//		time.Sleep(time.Second * 10)
		//Reload job
		r.job, err = r.cl.Get(r.name, r.ns)
		if err != nil {
			return err
		}
		//We should not make progress on any of these statuses
		switch r.job.Status {
		case "":
			return fmt.Errorf("Uninitialized job")
		case job.StatusFail:
			fallthrough
		case job.StatusPending:
			fallthrough
		case job.StatusDeploying:
			fallthrough
		case job.StatusComplete:
			return fmt.Errorf(r.job.Status)
		}
	}
}

func (r *Runner) work(w JobWorker) error {
	//Preflight check for map jobs
	for id, task := range r.job.Maps {
		if task.Worker == "" {
			//Try to run this task...
			return r.runMap(w, id)
		}
	}
	//Reached here means maps have finished, and job has been populated fresh
	if r.job.Reduces == nil {
		reduceinputs := make(map[int][]string)
		reducejobs := make(map[int]job.ReduceTask)
		for _, v := range r.job.Maps {
			//Double check all maps are finished, across all workers
			if v.Status != job.StatusComplete {
				//Silently fail
				log.Warn("Map is not complete")
				time.Sleep(time.Second * 20) //To not spam the api server while waiting for turtle worker
				return nil
			}
			for part, outputs := range v.Outputs {
				_, ok := reduceinputs[part]
				if ok {
					reduceinputs[part] = append(reduceinputs[part], outputs)
				} else {
					reduceinputs[part] = []string{outputs}
				}
			}
		}
		for part, v := range reduceinputs {
			reducejobs[part] = job.ReduceTask{Inputs: v}
		}
		//Try and stamp this into the job...
		patchObj := jsonpatch.New()
		patchObj = patchObj.Add("test", "/reduces", nil)
		patchObj = patchObj.Add("add", "/reduces", reducejobs)
		err := r.cl.PatchJob(r.name, r.ns, patchObj)
		if err != nil {
			//Somebody likely beat the worker to the race.
			//Silently fail
			log.Info(err)
			return nil
		}
	}
	//OK so reduce jobs are populated...
	for id, task := range r.job.Reduces {
		if task.Worker == "" {
			//Try to run this task...
			return r.runReduce(w, id)
		}
	}
	//Check if all reduces are finished...
	if r.job.Status == job.StatusReduce {
		results := make([]string, 0)
		for _, v := range r.job.Reduces {
			if v.Status != job.StatusComplete {
				//Silently fail
				log.Warn("Reduce is not complete")
				time.Sleep(time.Second * 20) //To not spam the api server while waiting for turtle worker
				return nil
			}
			if v.Output != "" {
				//Blank output indicates no result, but successfull job
				results = append(results, v.Output)
			}
		}
		//TODO: Do a GC if requested by user
		//Try and stamp this into the job...
		patchObj := jsonpatch.New()
		patchObj = patchObj.Add("test", "/results", nil)
		patchObj = patchObj.Add("add", "/results", results)
		patchObj = patchObj.Add("replace", "/status", job.StatusComplete)
		err := r.cl.PatchJob(r.name, r.ns, patchObj)
		if err != nil {
			//Somebody likely beat the worker to the race.
			//Silently fail
			log.Info(err)
			return nil
		}
	}
	return nil
}

func (r *Runner) runReduce(w JobWorker, id int) error {
	//Aquire lock
	patchObj := jsonpatch.New()
	patchObj = patchObj.Add("test", fmt.Sprintf("/reduces/%v/worker", id), "")
	patchObj = patchObj.Add("add", fmt.Sprintf("/reduces/%v/worker", id), r.hostname)
	patchObj = patchObj.Add("add", fmt.Sprintf("/reduces/%v/status", id), job.StatusProgress)
	patchObj = patchObj.Add("replace", "/status", job.StatusReduce)
	err := r.cl.PatchJob(r.name, r.ns, patchObj)
	if err != nil {
		//Couldnt aquire lock, lets not report this error
		log.Info(err)
		return nil
	}
	//OK lock aquired run map
	output, err := w.Reduce(id, r.job.Reduces[id].Inputs, r.jobArgs, r.secrets, r.utils)
	if err != nil {
		log.Error(err)
		//Stamp err
		patchObj = jsonpatch.New()
		patchObj = patchObj.Add("add", fmt.Sprintf("/reduces/%v/error", id), err.Error())
		patchObj = patchObj.Add("add", fmt.Sprintf("/reduces/%v/status", id), job.StatusFail)
		patchObj = patchObj.Add("add", "/status", job.StatusFail)
		patchObj = patchObj.Add("add", "/error", fmt.Sprintf("reduces(%v) %s", id, err.Error()))
		err = r.cl.PatchJob(r.name, r.ns, patchObj)
		return err
	}
	//OK success!
	for i := 0; i < 10; i++ {
		err = r.cl.PatchJob(r.name, r.ns, jsonpatch.New().Add("replace", fmt.Sprintf("/reduces/%v/output", id), output).Add("add", fmt.Sprintf("/reduces/%v/status", id), job.StatusComplete))
		if err != nil {
			log.Warn(err)
			log.Info("Retrying", i)
			time.Sleep(time.Second * time.Duration(i) * 2)
		} else {
			return nil
		}
	}

	return err
}

func (r *Runner) runMap(w JobWorker, id int) error {
	//Aquire lock
	patchObj := jsonpatch.New()
	patchObj = patchObj.Add("test", fmt.Sprintf("/maps/%v/worker", id), "")
	patchObj = patchObj.Add("add", fmt.Sprintf("/maps/%v/worker", id), r.hostname)
	patchObj = patchObj.Add("add", fmt.Sprintf("/maps/%v/status", id), job.StatusProgress)
	patchObj = patchObj.Add("replace", "/status", job.StatusMap)
	err := r.cl.PatchJob(r.name, r.ns, patchObj)
	if err != nil {
		//Couldnt aquire lock, lets not report this error
		log.Info(err)
		return nil
	}
	//OK lock aquired run map
	outputs, err := w.Map(id, r.job.Maps[id].Input, r.jobArgs, r.secrets, r.utils)
	if err != nil {
		log.Error(err)
		//Stamp err
		patchObj = jsonpatch.New()
		patchObj = patchObj.Add("add", fmt.Sprintf("/maps/%v/error", id), err.Error())
		patchObj = patchObj.Add("add", fmt.Sprintf("/maps/%v/status", id), job.StatusFail)
		patchObj = patchObj.Add("add", "/status", job.StatusFail)
		patchObj = patchObj.Add("add", "/error", fmt.Sprintf("map(%v) %s", id, err.Error()))
		err = r.cl.PatchJob(r.name, r.ns, patchObj)
		return err
	}
	//OK success!

	//OK success!
	for i := 0; i < 10; i++ {
		err = r.cl.PatchJob(r.name, r.ns, jsonpatch.New().Add("add", fmt.Sprintf("/maps/%v/outputs", id), outputs).Add("add", fmt.Sprintf("/maps/%v/status", id), job.StatusComplete))
		if err != nil {
			log.Warn(err)
			log.Info("Retrying", i)
			time.Sleep(time.Second * time.Duration(i) * 2)
		} else {
			return nil
		}
	}
	return err
}
