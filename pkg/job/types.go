package job

import (
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/turbobytes/kubemr/pkg/jsonpatch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
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

//MapReduceJobList is result of rest api call
type MapReduceJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MapReduceJob `json:"items"`
}

// MapReduceJob defines TPR object for a map-reduce job
type MapReduceJob struct {
	metav1.TypeMeta `json:",inline"`
	api.ObjectMeta  `json:"metadata,omitempty"`
	Spec            Spec               `json:"spec"`
	Status          string             `json:"status"`
	Err             string             `json:"error"`
	Maps            map[int]MapTask    `json:"maps"`
	Reduces         map[int]ReduceTask `json:"reduces"`
	Results         []string           `json:"results"`
}

//GetJobArgs returns job arguments
func (jb *MapReduceJob) GetJobArgs() map[string]string {
	return jb.Spec.spec.JobArgs
}

//initialize returns the patch specification for a new job discovered by operator
func (jb *MapReduceJob) initialize() jsonpatch.Patch {
	if jb.Status != "" {
		//Abort early if job is already initialized
		log.Infof("Job status is %s, aborting", jb.Status)
		return nil
	}
	err := jb.Spec.Validate()
	if err != nil {
		return jb.specfail()
	}
	return jb.Spec.PatchSpecPending()
}

//specfail returns the patch object for failure
func (jb *MapReduceJob) specfail() jsonpatch.Patch {
	updateobj := jsonpatch.New()
	//Only proceed with specfail if status is nil because we are initializing here...
	updateobj = updateobj.Add("test", "/status", nil)
	updateobj = updateobj.Add("add", "/status", StatusFail)
	updateobj = updateobj.Add("add", "/err", jb.Spec.err.Error())
	return updateobj
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

//Spec is the job specification outer container
type Spec struct {
	spec *jobspec
	err  error
}

//Validate Sends the validation error of this spec
func (spec *Spec) Validate() error {
	if spec.err != nil {
		return spec.err
	}
	if spec.spec == nil {
		return fmt.Errorf("Spec is required")
	}
	return nil
}

//PatchSpecPending returns patch specification for pending job
func (spec *Spec) PatchSpecPending() jsonpatch.Patch {
	patches := jsonpatch.New()
	patches = patches.Add("test", "/status", nil)
	inputs := make(map[int]MapTask)
	for i, input := range spec.spec.Inputs {
		inputs[i] = MapTask{Input: input}
	}
	patches = patches.Add("add", "/maps", inputs)
	patches = patches.Add("add", "/status", StatusPending)
	return patches
}

//Doing this as workaround to silently fail
type jobspec struct {
	Image    string            `json:"image"`    //The image that runs the job
	Replicas *int32            `json:"replicas"` //Number of workers to run in parallel
	KeepTmp  bool              `json:"keeptmp"`  //Keep intermediate stage files
	Inputs   []string          `json:"inputs"`   //List of initial inputs for the map phase
	JobArgs  map[string]string `json:"jobargs"`  //Arbitary optional arguments which might make sense to user defined worker
	//UserSecretName string              `json:"usersecretname"` //Optional: Name of secret in job's namespace to be available to worker
	Template *v1.PodTemplateSpec `json:"template"` //Pod template for the job
	//TODO
}

//UnmarshalJSON silently fails on error
//Kubernetes seems to not provide any way to validate TPR object schema...
func (spec *Spec) UnmarshalJSON(b []byte) error {
	spec.spec = &jobspec{}
	spec.err = json.Unmarshal(b, spec.spec)
	if spec.err != nil {
		//Silent fail...
		spec.spec = nil
	} else {
		//Do some validations... if fail populate error and make spec nil
		spec.err = spec.spec.Validate()
		if spec.err != nil {
			spec.spec = nil
		}
	}
	return nil
}

//Validate validates the incoming jobspec
func (spec *jobspec) Validate() error {
	if spec.Image == "" {
		return fmt.Errorf("Image must be provided")
	}
	if len(spec.Inputs) == 0 {
		return fmt.Errorf("Atleast 1 input needed")
	}
	if spec.Template == nil {
		return fmt.Errorf("Template is required")
	}
	if spec.Replicas == nil {
		spec.Replicas = &defaultreplica
	}
	return nil
}

//MarshalJSON packs job
func (spec *Spec) MarshalJSON() ([]byte, error) {
	if spec.spec == nil {
		return nil, nil
	}
	return json.Marshal(spec.spec)
}
