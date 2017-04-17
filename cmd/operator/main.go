package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/onrik/logrus/filename"
	"github.com/turbobytes/kubemr/pkg/job"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/unversioned"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	kubeconfig *string
)

func init() {
	//Set this for testing purposes... in prod this would always be in-cluster
	kubeconfig = flag.String("kubeconfig", "", "path to kubeconfig, if absent then we use rest.InClusterConfig()")
	flag.Parse()
	//log.SetFormatter(&log.JSONFormatter{})
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)
}

func ensureTprExists(cl *kubernetes.Clientset) {
	tpr, err := cl.ExtensionsV1beta1().ThirdPartyResources().Get("map-reduce-job.turbobytes.com")
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
	jobclient *job.Client
}

func newjobmanager(config *rest.Config, cl *kubernetes.Clientset) (*jobmanager, error) {
	j := &jobmanager{}
	groupversion := unversioned.GroupVersion{
		Group:   "turbobytes.com",
		Version: "v1alpha1",
	}
	config.APIPath = "/apis"
	config.GroupVersion = &groupversion
	dynclient, err := dynamic.NewClient(config)
	if err != nil {
		return nil, err
	}
	j.jobclient = job.NewClient(dynclient)
	return j, nil
}

func (j *jobmanager) jobloop() error {
	return j.jobclient.WatchList()
}

func getconfig() (*rest.Config, error) {
	if *kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", *kubeconfig)
}

func main() {
	log.Info("Operator starting...")
	// creates the in-cluster config
	config, err := getconfig()
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
