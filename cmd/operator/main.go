package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
	"github.com/onrik/logrus/filename"
	"github.com/turbobytes/kubemr/pkg/job"
	k8s "github.com/turbobytes/kubemr/pkg/k8s.go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

var (
	kubeconfig   = flag.String("kubeconfig", "", "path to kubeconfig, if absent then we use rest.InClusterConfig()")
	s3region     = flag.String("s3region", "ap-southeast-1", "The S3 region we wanna use for temporary stuff")
	bucketname   = flag.String("bucketname", "kubemr", "A pre-existing bucket")
	bucketprefix = flag.String("bucketprefix", "", "Prepended to all keys, to reduce clutter in bucket root")
	apiserver    = flag.String("apiserver", "", "Url to apiserver, blank to read from kubeconfig")
)

func init() {
	//Set this for testing purposes... in prod this would always be in-cluster
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
	//TODO: Wait for the TPR to be initialized
	log.Info(tpr)
}

func main() {
	log.Info("Operator starting...")
	// creates the in-cluster config
	config, err := k8s.GetConfig(*apiserver, *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	ensureTprExists(clientset)
	jobclient, err := job.NewClient(config, &job.Config{
		S3Region:     *s3region,
		BucketName:   *bucketname,
		BucketPrefix: *bucketprefix,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(jobclient.WatchList())
	//managejob(clientset)
}
