package job

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nbari/violetear"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	batchv1 "k8s.io/client-go/pkg/apis/batch/v1"
)

var (
	defaultreplica int32 = 1
)

const (
	//StatusFail when job fails
	StatusFail = "FAIL"
	//StatusPending when job has been validated, but DS has not been created
	StatusPending = "PENDING"
	//StatusDeploying when job related resources are being deployed. Used as internal lock
	StatusDeploying = "DEPLOYING"
	//StatusDeployed when job related resources have been created in the desired namespace
	StatusDeployed = "DEPLOYED"
	//StatusMap when job is in map phase
	StatusMap = "MAP"
	//StatusReduce when job is in reduce phase
	StatusReduce = "REDUCE"
	//StatusComplete when job has been processed successfully
	StatusComplete = "COMPLETE"
	//StatusProgress when job is in progress
	StatusProgress = "PROGRESS"
)

// MapReduceJob defines TPR object for a map-reduce job
type MapReduceJob struct {
	*sync.RWMutex
	Name      string             `json:"name"`      //Name generated by system
	Namespace string             `json:"namespace"` //Name generated by system
	Status    string             `json:"status"`    //Status of the job
	Err       string             `json:"error"`     //Errors, if any
	Maps      map[int]MapTask    `json:"maps"`
	Reduces   map[int]ReduceTask `json:"reduces"`
	Results   []string           `json:"results"`
	Replicas  *int32             `json:"replicas"` //Number of workers to run in parallel
	Inputs    []string           `json:"inputs"`   //List of initial inputs for the map phase
	//UserSecretName string              `json:"usersecretname"` //Optional: Name of secret in job's namespace to be available to worker
	Template v1.PodTemplateSpec `json:"template"` //Pod template for the job
	server   *http.Server
	poke     chan bool
	cl       kubernetes.Interface //k8s client to do Kubernetes things
	addr     string
	config   *Config
	jobname  string //Store the job name generated by kubernetes
}

//Init initializes the job, setting sane defaults
func (jb *MapReduceJob) Init(cl kubernetes.Interface, addr, myip string, cfg *Config) error {
	jb.cl = cl
	jb.RWMutex = &sync.RWMutex{}
	//Do not generate uuid... let k8s handle that...
	//Validate name
	if jb.Name == "" {
		return fmt.Errorf("A name must be provided")
	}
	//Stamp unique name
	//jb.Name = jb.Name + "-" + strings.ToLower(s)
	jb.Status = StatusPending
	if jb.Replicas == nil {
		jb.Replicas = &defaultreplica
	}
	jb.Maps = make(map[int]MapTask)
	jb.Reduces = make(map[int]ReduceTask)
	//if jb.Template == nil {
	//return fmt.Errorf("Template not provided")
	//}
	if jb.Inputs == nil || len(jb.Inputs) == 0 {
		return fmt.Errorf("Inputs not provided")
	}
	if jb.Namespace == "" {
		jb.Namespace = "default"
	}
	jb.poke = make(chan bool, 10) //Creating some buffer otherwise unlock defer doesnt work when channel is full
	jb.addr = addr
	//Populate the config
	cfg.JobURL = fmt.Sprintf("http://%s%s/%s/", myip, addr, jb.Name)
	if !strings.HasSuffix(cfg.BucketPrefix, "/") {
		cfg.BucketPrefix = cfg.BucketPrefix + "/"
	}
	cfg.BucketPrefix = cfg.BucketPrefix + jb.Name + "/"
	jb.config = cfg
	return jb.deployk8()
}

//deploy the batch job
func (jb *MapReduceJob) deployk8() error {
	//Hmm... instead of spamming k8 with secrets we let user handle KUBEMR_S3_ACCESS_KEY_ID and KUBEMR_S3_SECRET_ACCESS_KEY
	//Annotate the podspec
	podspec := jb.Template
	podspec.Spec.RestartPolicy = v1.RestartPolicyOnFailure
	//Create Batch job...
	for i := range podspec.Spec.Containers {
		podspec.Spec.Containers[i].Env = append(podspec.Spec.Containers[i].Env, stampCommonEnv(jb.config)...)
	}
	//Prepare the batch job
	jobspec := batchv1.Job{
		//Metadata
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: jb.Name,
			Namespace:    jb.Namespace,
		},
		//Jobspec
		Spec: batchv1.JobSpec{
			Parallelism: jb.Replicas,
			//Pod template
			Template: podspec,
		},
	}
	//Deploy the job...
	j, err := jb.cl.BatchV1().Jobs(jobspec.Namespace).Create(&jobspec)
	if err != nil {
		jb.jobname = j.Name
		log.Infof("Created job: %s", j.Name)
	}
	return err
}

//Start deploys the job and starts the server
func (jb *MapReduceJob) Start(timeout time.Duration) error {
	//Populate maps
	for i, input := range jb.Inputs {
		jb.Maps[i] = MapTask{Input: input}
	}

	//FAKE status
	jb.Status = StatusMap
	router := violetear.New()
	router.LogRequests = true
	router.RequestID = "Request-ID"
	router.AddRegex(":taskid", `[0-9]+`) //String one or more chr

	//Get the whole job
	router.HandleFunc("/"+jb.Name+"/", jb.handleGet, "GET")
	router.HandleFunc("/"+jb.Name+"/map/:taskid/", jb.handleMap, "PUT")
	router.HandleFunc("/"+jb.Name+"/reduce/:taskid/", jb.handleReduce, "PUT")
	jb.server = &http.Server{
		Handler:        router,
		Addr:           jb.addr,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	//How to send err?
	go jb.server.ListenAndServe()
	return jb.wait(timeout)
}

func (jb *MapReduceJob) stop() {
	err := jb.server.Shutdown(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func (jb *MapReduceJob) cleanup() {
	//Delete the batch job in k8s
	if jb.jobname != "" {
		log.Info("Deleting batch job")
		err := jb.cl.BatchV1().Jobs(jb.Namespace).Delete(jb.jobname, &metav1.DeleteOptions{})
		if err != nil {
			log.Error(err)
		}
	}
	//Perhaps hunt for lingering pods...
}

//Single loop of checking job status
func (jb *MapReduceJob) jobloop() (bool, error) {
	jb.Lock()
	defer jb.Unlock()
	log.Info(jb.Status)
	switch jb.Status {
	case "":
		//Should not be
		return true, fmt.Errorf("Status is still pending")
	case StatusPending:
		//Should not be
		return true, fmt.Errorf("Status is still unknown")
	case StatusFail:
		return true, fmt.Errorf(jb.Err)
	case StatusComplete:
		return true, nil
	case StatusDeployed:
		//TODO: Check if maps are populated...
	case StatusMap:
		//Check if its ready for reduce... or err
		alldone := true
		reduces := make(map[int][]string)
		for taskid, m := range jb.Maps {
			if m.Status == StatusFail {
				//One of the maps had a fail... Fail the whole job
				jb.Status = StatusFail
				jb.Err = fmt.Sprintf("MAP: Worker: %s, Task: %v, Err: %s", m.Worker, taskid, m.Err)
				return true, fmt.Errorf(jb.Err)
			}
			alldone = alldone && m.Status == StatusComplete
			if m.Outputs != nil {
				for redid, output := range m.Outputs {
					reduces[redid] = append(reduces[redid], output)
				}
			}
		}
		//alldone will be true only if each of the maps have finished
		if alldone {
			for taskid, inputs := range reduces {
				jb.Reduces[taskid] = ReduceTask{Inputs: inputs}
			}
			jb.Status = StatusReduce
		}
	case StatusReduce:
		//Check if its finished or err
		alldone := true
		results := make([]string, 0)
		for taskid, r := range jb.Reduces {
			if r.Status == StatusFail {
				//One of the maps had a fail... Fail the whole job
				jb.Status = StatusFail
				jb.Err = fmt.Sprintf("REDUCE: Worker: %s, Task: %v, Err: %s", r.Worker, taskid, r.Err)
				return true, fmt.Errorf(jb.Err)
			}
			alldone = alldone && r.Status == StatusComplete
			if r.Output != "" {
				results = append(results, r.Output)
			}
		}
		if alldone {
			jb.Status = StatusComplete
			jb.Results = results
			return true, nil
		}
	}
	return false, nil
}

//Wait until job is ober...
func (jb *MapReduceJob) wait(timeout time.Duration) error {
	defer jb.stop()    //Stop the server once we exit...
	defer jb.cleanup() //Remove k8s resources
	defer close(jb.poke)
	t := time.After(timeout)
	for {
		select {
		case <-jb.poke:
			log.Info("Poked")
			done, err := jb.jobloop()
			if done {
				return err
			}
		case <-t:
			return fmt.Errorf("Job timed out after %s", timeout)
		}
	}
}

//Result describes the final result to be consumed by the user
type Result string

//ReduceTask holds the values for individual map task
type ReduceTask struct {
	Worker string   `json:"worker"` //Hostname, used for locking
	Inputs []string `json:"inputs"` //Multiple possible inputs for a reduce job
	Output string   `json:"output"` //Single output from reduce
	Err    string   `json:"error"`
	Status string   `json:"status"`
}

//MapTask holds the values for individual map task
type MapTask struct {
	Worker  string         `json:"worker"`  //Hostname, used for locking
	Input   string         `json:"input"`   //One input per map
	Outputs map[int]string `json:"outputs"` //Multiple possible outputs
	Err     string         `json:"error"`
	Status  string         `json:"status"`
}
