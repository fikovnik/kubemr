package job

import (
	"encoding/json"
	"fmt"

	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/unversioned"
)

var (
	defaultreplica int32 = 1
)

const (
	//StatusFail when job fails
	StatusFail = "FAIL"
	//StatusPending when job has been validated, but DS has not been created
	StatusPending = "PENDING"
)

//MapReduceJobList is result of rest api call
type MapReduceJobList struct {
	unversioned.TypeMeta `json:",inline"`
	unversioned.ListMeta `json:"metadata,omitempty"`
	Items                []MapReduceJob `json:"items"`
}

// MapReduceJob defines TPR object for a map-reduce job
type MapReduceJob struct {
	unversioned.TypeMeta `json:",inline"`
	api.ObjectMeta       `json:"metadata,omitempty"`
	Spec                 Spec               `json:"spec"`
	Status               string             `json:"status"`
	Err                  string             `json:"error"`
	Maps                 map[int]MapTask    `json:"maps"`
	Reduces              map[int]ReduceTask `json:"reduces"`
	Results              []Result           `json:"results"`
}

//Result describes the final result to be consumed by the user
type Result struct {
}

//ReduceTask holds the values for individual map task
type ReduceTask struct {
	Worker string   `json:"worker"` //Hostname, used for locking
	Inputs []string `json:"inputs"` //Multiple possible inputs for a reduce job
	Output string   `json:"output"` //Single output from reduce
	Err    error    `json:"error"`
}

//MapTask holds the values for individual map task
type MapTask struct {
	Worker  string   `json:"worker"`  //Hostname, used for locking
	Input   string   `json:"input"`   //One input per map
	Outputs []string `json:"outputs"` //Multiple possible outputs
	Err     error    `json:"error"`
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
func (spec *Spec) PatchSpecPending() []map[string]interface{} {
	patches := make([]map[string]interface{}, 3)
	patches[0] = map[string]interface{}{"op": "test", "path": "/status", "value": nil} //Only update things with no status
	inputs := make(map[int]MapTask)
	for i, input := range spec.spec.Inputs {
		inputs[i] = MapTask{Input: input}
	}
	patches[1] = map[string]interface{}{"op": "add", "path": "/maps", "value": inputs}          //Populate the map inputs
	patches[2] = map[string]interface{}{"op": "add", "path": "/status", "value": StatusPending} //Update the status
	return patches
}

//Doing this as workaround to silently fail
type jobspec struct {
	Image    string                 `json:"image"`    //The image that runs the job
	Replicas *int32                 `json:"replicas"` //Number of workers to run in parallel
	KeepTmp  bool                   `json:"keeptmp"`  //Keep intermediate stage files
	Inputs   []string               `json:"inputs"`   //List of initial inputs for the map phase
	JobArgs  map[string]interface{} //Arbitary optional arguments which might make sense to user defined worker
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
