package main

import (
	"encoding/json"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/onrik/logrus/filename"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/api/unversioned"
)

var config *rest.Config
var defaultreplica int32 = 1

const (
	//StatusFail when job fails
	StatusFail = "FAIL"
	//StatusPending when job has been validated, but DS has not been created
	StatusPending = "PENDING"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)
}

//MapReduceJobList is result of rest api call
type MapReduceJobList struct {
	unversioned.TypeMeta `json:",inline"`
	unversioned.ListMeta `json:"metadata,omitempty"`
	Items                []MapReduceJob `json:"items"`
}

// MapReduceJob defines TPR object for a map-reduce job
type MapReduceJob struct {
	metav1.TypeMeta `json:",inline"`
	v1.ObjectMeta   `json:"metadata,omitempty"`
	Spec            JobSpec `json:"spec"`
	Status          string  `json:"status"`
	Err             string  `json:"error"`
}

//JobSpec is the job specification outer container
type JobSpec struct {
	spec *jobspec
	err  error
}

//Doing this as workaround to silently fail
type jobspec struct {
	Template v1.PodTemplateSpec `json:"template" protobuf:"bytes,3,opt,name=template"`
	Replicas *int32             `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
	KeepTmp  bool               `json:"keeptmp"` //Keep intermediate stage files
	//TODO
}

//UnmarshalJSON silently fails on error
func (s *JobSpec) UnmarshalJSON(b []byte) error {
	s.spec = &jobspec{}
	s.err = json.Unmarshal(b, s.spec)
	if s.err != nil {
		//Silent fail...
		s.spec = nil
	} else {
		if s.spec.Replicas == nil {
			s.spec.Replicas = &defaultreplica
		}
		//TODO: Do some validations... if fail populate error and make spec nil
	}
	return nil
}

//MarshalJSON packs job
func (s *JobSpec) MarshalJSON() ([]byte, error) {
	if s.spec == nil {
		return nil, nil
	}
	return json.Marshal(s.spec)
}

func ensureTprExists(cl *kubernetes.Clientset) {
	tpr, err := cl.ExtensionsV1beta1().ThirdPartyResources().Get("map-reduce-job.turbobytes.com", metav1.GetOptions{})
	if err == nil {
		log.Info("TPR exists")
		return
	}
	//Create TPR
	tpr = &v1beta1.ThirdPartyResource{
		Description: "Map reduce job specification",
		Versions:    []v1beta1.APIVersion{v1beta1.APIVersion{Name: "v1alpha1"}},
	}
	tpr.Kind = "ThirdPartyResource"
	tpr.APIVersion = "extensions/v1beta1"
	tpr.Name = "map-reduce-job.turbobytes.com"
	//Try to insert TPR
	tpr, err = cl.ExtensionsV1beta1().ThirdPartyResources().Create(tpr)
	if err != nil {
		log.Fatal(err)
	}
	log.Info(tpr)
}

type jobmanager struct {
	cl        *kubernetes.Clientset
	tprclient *rest.RESTClient
}

func newjobmanager(config *rest.Config, cl *kubernetes.Clientset) (*jobmanager, error) {
	j := &jobmanager{
		cl: cl,
	}
	groupversion := schema.GroupVersion{
		Group:   "turbobytes.com",
		Version: "v1alpha1",
	}
	config.APIPath = "/apis"
	config.GroupVersion = &groupversion
	config.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	tprclient, err := rest.RESTClientFor(config)
	if err != nil {
		return nil, err
	}
	j.tprclient = tprclient
	return j, nil
}

func (j *jobmanager) jobloop() error {
	for {
		err := j.jobloopSingle()
		if err != nil {
			//TODO: Return error if something critical happens
			log.Error(err)
		}
		time.Sleep(time.Second * 15)
	}
}

func (j *jobmanager) jobloopSingle() error {
	req := j.tprclient.Get().Resource("mapreducejobs")
	log.Info(req.URL())
	jobList := MapReduceJobList{}

	b, err := req.DoRaw()
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &jobList)
	if err != nil {
		return err
	}
	for _, job := range jobList.Items {
		j.process(job)
	}
	return nil
}

func (j *jobmanager) process(job MapReduceJob) error {
	switch job.Status {
	case "":
		return j.checkspec(job)
	}
	return nil
}

func (j *jobmanager) checkspec(job MapReduceJob) error {
	if job.Spec.spec == nil {
		return j.specfail(job)
	}
	//Spec is ok, lets stamp pending
	return j.patchjob(job, map[string]interface{}{"status": StatusPending})
}

func (j *jobmanager) specfail(job MapReduceJob) error {
	if job.Status != StatusFail {
		updateobj := make(map[string]interface{})
		updateobj["status"] = StatusFail
		if job.Spec.err != nil {
			updateobj["err"] = job.Spec.err.Error()
		} else {
			updateobj["err"] = "Spec is required"
		}
		return j.patchjob(job, updateobj)
	}
	return nil
}

//Since we use patch with only the fields we wanna update,
//it shouldnt cause issues if multiple operators are doing the same thing.
func (j *jobmanager) patchjob(job MapReduceJob, update map[string]interface{}) error {
	b, err := json.Marshal(update)
	if err != nil {
		return err
	}
	req := j.tprclient.Patch(types.MergePatchType).Resource("mapreducejobs").Namespace(job.Namespace).Name(job.Name).Body(b)
	log.Info(req.URL())
	b, err = req.DoRaw()
	if err != nil {
		log.Info(string(b))
		return err
	}
	return nil
}

func main() {
	log.Info("Operator starting...")
	// creates the in-cluster config
	var err error
	config, err = rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	ensureTprExists(clientset)
	j, err := newjobmanager(config, clientset)
	if err != nil {
		log.Fatal(err)
	}
	err = j.jobloop()
	if err != nil {
		log.Fatal(err)
	}
	//managejob(clientset)
}
