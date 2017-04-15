package main

import (
	"encoding/json"

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

//managejob loops thru TPR objects and adjusts status, deploy workers, etc
func managejob(cl *kubernetes.Clientset) {
	tprconfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	groupversion := schema.GroupVersion{
		Group:   "turbobytes.com",
		Version: "v1alpha1",
	}
	tprconfig.APIPath = "/apis"
	tprconfig.GroupVersion = &groupversion
	tprconfig.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	tprclient, err := rest.RESTClientFor(tprconfig)
	if err != nil {
		log.Fatal(err)
	}
	req := tprclient.Get().Resource("mapreducejobs")
	log.Info(req.URL())
	jobList := MapReduceJobList{}

	b, err := req.DoRaw()
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(b, &jobList)
	if err != nil {
		log.Fatal(err)
	}
	for _, job := range jobList.Items {
		log.Info(job.Name)
		if job.Spec.spec == nil {
			log.Info("spec is nil")
			//Update the TPR
			if job.Status == "" {
				job.Status = "FAIL"
				if job.Spec.err != nil {
					job.Err = job.Spec.err.Error()
				} else {
					job.Err = "Spec is required"
				}
				b, err := json.Marshal(job)
				if err != nil {
					log.Error(err)
				} else {
					req = tprclient.Patch(types.MergePatchType).Resource("mapreducejobs").Namespace(job.Namespace).Name(job.Name).Body(b)
					log.Info(req.URL())
					b, err = req.DoRaw()
					if err != nil {
						log.Error(err)
					}
					log.Info(string(b))
				}
			}
		} else {
			log.Info(job.Spec.spec)
		}

	}
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
	managejob(clientset)
}
