package worker

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"

	log "github.com/sirupsen/logrus"
	"github.com/turbobytes/kubemr/pkg/job"
)

//Runner manages the lifecycle of a worker
type Runner struct {
	job      *job.MapReduceJob
	cl       *job.Client
	name, ns string
	hostname string //for locking, debugging
	utils    *Utilities
}

//NewRunner initializes things from enviornment and returns a NewRunner
func NewRunner() (*Runner, error) {
	cfg := job.NewConfigEnv()
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

	jobclient := job.NewClient(cfg.JobURL)
	var err error
	r := &Runner{}
	r.hostname, err = os.Hostname()
	if err != nil {
		return nil, err
	}
	r.cl = jobclient
	r.job, err = r.cl.GetJob()
	if err != nil {
		return nil, err
	}
	//Store the name, namespace
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
		return NewRunner()
	case job.StatusComplete:
		return nil, fmt.Errorf(r.job.Status)
	}

	//Ensure bucket exists
	bucket := s.Bucket(os.Getenv("KUBEMR_S3_BUCKET_NAME"))
	//TODO: Check before attempting put. Maybe this should be done elsewhere
	bucket.PutBucket("")
	r.utils = NewUtilities(bucket, cfg.BucketPrefix)
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
		r.job, err = r.cl.GetJob()
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
	switch r.job.Status {
	case job.StatusMap:
		for id, task := range r.job.Maps {
			if task.Worker == "" {
				//Try to run this task...
				return r.runMap(w, id)
			}
		}
		//Reached here means we are in map phase but everything is taken
		log.Warn("Map is not complete, but everything is taken")
		time.Sleep(time.Second * 5) //To not spam the api server while waiting for turtle worker
		return nil
	case job.StatusReduce:
		//OK so reduce jobs are populated...
		for id, task := range r.job.Reduces {
			if task.Worker == "" {
				//Try to run this task...
				return r.runReduce(w, id)
			}
		}
		//Reached here means we are in reduce phase but everything is taken
		log.Warn("Reduce is not complete, but everything is taken")
		time.Sleep(time.Second * 5) //To not spam the api server while waiting for turtle worker
		return nil
	}
	return nil
}

func (r *Runner) runReduce(w JobWorker, id int) error {
	task := r.job.Reduces[id]
	//Aquire lock
	task.Worker = r.hostname
	task.Status = job.StatusProgress
	ok, err := r.cl.PutReduce(task, id)
	if err != nil {
		return err
	}
	if !ok {
		//Job was not aquired...
		return nil
	}

	//OK lock aquired run reduce
	output, err := w.Reduce(id, task.Inputs, r.utils)
	if err != nil {
		log.Error(err)
		//Stamp err
		task.Status = job.StatusFail
		task.Err = err.Error()
		_, err = r.cl.PutReduce(task, id)

		return err
	}
	//OK success!
	task.Status = job.StatusComplete
	task.Output = output
	ok, err = r.cl.PutReduce(task, id)
	if !ok && err == nil {
		return fmt.Errorf("Something went terribly wrong, check logs for PutMap")
	}
	return err
}

func (r *Runner) runMap(w JobWorker, id int) error {
	task := r.job.Maps[id]
	//Aquire lock
	task.Worker = r.hostname
	task.Status = job.StatusProgress
	ok, err := r.cl.PutMap(task, id)
	if err != nil {
		return err
	}
	if !ok {
		//Job was not aquired...
		return nil
	}
	//OK. So now task jas been aquired and locked
	outputs, err := w.Map(id, r.job.Maps[id].Input, r.utils)
	if err != nil {
		log.Error(err)
		//Stamp err
		task.Status = job.StatusFail
		task.Err = err.Error()
		_, err = r.cl.PutMap(task, id)
		return err
	}
	//OK success!

	task.Status = job.StatusComplete
	task.Outputs = outputs
	ok, err = r.cl.PutMap(task, id)
	if !ok && err == nil {
		return fmt.Errorf("Something went terribly wrong, check logs for PutMap")
	}
	return err
}
