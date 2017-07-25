package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/turbobytes/kubemr/pkg/job"
	"github.com/turbobytes/kubemr/pkg/k8s"
)

var (
	kubeconfig   = flag.String("kubeconfig", "", "path to kubeconfig, if absent then we use rest.InClusterConfig()")
	s3region     = flag.String("s3region", "", "The S3 region we wanna use for temporary stuff")
	bucketname   = flag.String("bucketname", "", "A pre-existing bucket")
	bucketprefix = flag.String("bucketprefix", "", "Prepended to all keys, to reduce clutter in bucket root")
	apiserver    = flag.String("apiserver", "", "Url to apiserver, blank to read from kubeconfig")
	s3endpoint   = flag.String("s3endpoint", "", "The S3 endpoint we wanna use for temporary stuff(overrides region)")
)

func init() {
	flag.Parse()
}

func main() {
	j := `
  {
    "name": "wordcount",
    "inputs": ["https://tools.ietf.org/rfc/rfc4501.txt", "https://tools.ietf.org/rfc/rfc2017.txt", "https://tools.ietf.org/rfc/rfc2425.txt"],
    "replicas": 5,
    "template": {
      "spec": {
        "volumes":[{
          "name":"tmpdir",
          "emptyDir": {}
        }],
        "containers": [{
          "name": "kubemrworker",
          "image": "turbobytes/kubemr-wordcount:notpr",
          "imagePullPolicy": "Always",
          "volumeMounts": [{
            "name": "tmpdir",
            "mountPath": "/tmp"
          }],
          "env":[{
            "name": "KUBEMR_S3_ACCESS_KEY_ID",
            "valueFrom": {
              "secretKeyRef": {
                "name": "aws",
                "key": "aws_access_key_id"
              }
            }
          },{
            "name": "KUBEMR_S3_SECRET_ACCESS_KEY",
            "valueFrom": {
              "secretKeyRef": {
                "name": "aws",
                "key": "aws_secret_access_key"
              }
            }
          }]
        }]
      }
    }
  }
  `
	jb := &job.MapReduceJob{}
	err := json.Unmarshal([]byte(j), jb)
	if err != nil {
		panic(err)
	}
	config, err := k8s.GetConfig(*apiserver, *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	cl, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}
	cfg := job.NewConfigEnv()
	err = jb.Init(cl, ":8989", os.Getenv("MY_POD_IP"), cfg)
	if err != nil {
		panic(err)
	}
	err = jb.Start(time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(jb.Results)
}
