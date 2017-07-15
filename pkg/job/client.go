package job

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"

	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"

	log "github.com/sirupsen/logrus"
	"github.com/turbobytes/kubemr/pkg/jsonpatch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	batchv1 "k8s.io/client-go/pkg/apis/batch/v1"
	"k8s.io/client-go/rest"
)

var (
	jobResource = &metav1.APIResource{
		Name:       "mapreducejobs",
		Namespaced: true,
		Kind:       "MapReduceJobs",
	}
)

//Client manages all access to *Job types
type Client struct {
	tprclient *dynamic.Client
	jobclient *dynamic.ResourceClient
	clientset *kubernetes.Clientset
	secrets   map[string]string //Holds secret data that both operator and worker needs to do its thing.
	config    *Config           //Config for workers (and operator)
	bucket    *s3.Bucket        //S3 bucket
}

//NewClient creates a new client
func NewClient(k8sconfig *rest.Config, config *Config) (*Client, error) {
	// creates the clientset for normal kube operations
	clientset, err := kubernetes.NewForConfig(k8sconfig)
	if err != nil {
		return nil, err
	}
	//Update config for tprclient
	groupversion := schema.GroupVersion{
		Group:   "turbobytes.com",
		Version: "v1beta1",
	}
	k8sconfig.APIPath = "/apis"
	k8sconfig.GroupVersion = &groupversion

	//Create dynamic client for TPR operations
	tprclient, err := dynamic.NewClient(k8sconfig)
	if err != nil {
		return nil, err
	}
	cl := &Client{tprclient: tprclient, clientset: clientset}
	err = config.Validate()
	if err != nil {
		return nil, err
	}
	cl.config = config
	//Blank namespace for --all-namespaces
	//Operations on individual Job needs their own namespaced ResourceClient
	cl.jobclient = cl.tprclient.Resource(jobResource, "")
	cl.secrets = make(map[string]string)
	cl.secrets["S3_ACCESS_KEY_ID"] = os.Getenv("KUBEMR_S3_ACCESS_KEY_ID")
	cl.secrets["S3_SECRET_ACCESS_KEY"] = os.Getenv("KUBEMR_S3_SECRET_ACCESS_KEY")

	//Prepare S3 bucket
	auth := aws.Auth{
		AccessKey: os.Getenv("KUBEMR_S3_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("KUBEMR_S3_SECRET_ACCESS_KEY"),
	}

	region, ok := aws.Regions[config.S3Region]
	if !ok {
		return nil, fmt.Errorf("Unable to load S3 region")
	}
	if config.S3Endpoint != "" {
		region.S3Endpoint = config.S3Endpoint
	}
	s := s3.New(auth, region)
	cl.bucket = s.Bucket(config.BucketName)
	//Silently ignoring the Put for now
	err = cl.bucket.PutBucket("") //Ensure bucket exists
	log.Warn(err)
	return cl, nil
}

//Get a MapReduceJob by name
func (cl *Client) Get(name, ns string) (*MapReduceJob, error) {
	rescl := cl.tprclient.Resource(jobResource, ns)
	got, err := rescl.Get(name)
	if err != nil {
		return nil, err
	}
	return UnstructToMapReduceJob(got)
}

//Delete a MapReduceJob by name
func (cl *Client) Delete(name, ns string) error {
	rescl := cl.tprclient.Resource(jobResource, ns)
	return rescl.Delete(name, &metav1.DeleteOptions{})
}

//Create a new mapreduce job
func (cl *Client) Create(ns string, task map[string]interface{}) (*MapReduceJob, error) {
	in := &unstructured.Unstructured{Object: task}
	got, err := cl.tprclient.Resource(jobResource, ns).Create(in)
	if err != nil {
		return nil, err
	}
	return UnstructToMapReduceJob(got)
}

//List all jobs
func (cl *Client) List() (MapReduceJobList, error) {
	jobList := MapReduceJobList{}
	in, err := cl.jobclient.List(metav1.ListOptions{})
	if err != nil {
		log.Info(err)
		return jobList, err
	}
	b, err := json.Marshal(in)
	if err != nil {
		return jobList, err
	}
	err = json.Unmarshal(b, &jobList)
	return jobList, err //TODO
}

//Watch monitors single job by name
func (cl *Client) Watch(jobname, namespace string) {
	//TODO
}

//WatchList monitors all jobs
func (cl *Client) WatchList() error {
	openWatcher := NewJobWatcher(cl.jobclient)
	for {
		watcher, err := openWatcher()
		if err != nil {
			return err
		}
		defer watcher.Stop()
		watchCh := watcher.ResultChan()
		for evt := range watchCh {
			log.Printf("received event %#v", evt)
			jb, ok := evt.Object.(*MapReduceJob)
			if !ok {
				log.Printf("event was not a *MapReduceJob, skipping")
				continue
			}
			log.Printf("Job %s, status %s", jb.Name, jb.Status)
			switch evt.Type {
			case watch.Added:
				fallthrough
			case watch.Modified:
				switch jb.Status {
				case "":
					//Pre-flight checks, early abort
					err := cl.PatchJob(jb.Name, jb.Namespace, jb.initialize())
					if err != nil {
						log.Error(err)
					}
				case StatusPending:
					err := cl.Deploy(jb)
					if err != nil {
						log.Warn(err)
					}
					//TODO: Proceed with creating
				}
				//TODO: check/update progress/etc
			case watch.Deleted:
				//clear resources associated with it
				err := cl.clearjob(jb)
				if err != nil {
					log.Error(err)
				}
			}
		}
	}
}

//clearjob deletes the job
func (cl *Client) clearjob(jb *MapReduceJob) error {
	//Delete the associated Job data in S3
	policy := metav1.DeletePropagationForeground
	err := cl.clientset.BatchV1().Jobs(jb.Namespace).Delete(jb.Name, &metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil {
		return err
	}
	//Delete pods
	pods, err := cl.clientset.CoreV1().Pods(jb.Namespace).List(metav1.ListOptions{LabelSelector: "job-name=" + jb.Name})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		err = cl.clientset.CoreV1().Pods(jb.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
		if err != nil {
			return err
		}
	}
	//Delete S3 assets..
	go cl.GC() //Trigger GC in goroutinue.
	return err
}

//GC deletes old jobs, S3 files, pods, etc
func (cl *Client) GC() {
	//Prepare list of existing jobs
	jobs, err := cl.List()
	if err != nil {
		log.Infoln(err)
		return
	}
	names := make(map[string]bool)
	for _, jb := range jobs.Items {
		names[jb.Name] = true
	}
	log.Info(names)
	res, err := cl.bucket.List(cl.config.BucketPrefix, "/", "", 1000)
	if err != nil {
		log.Infoln(err)
		return
	}
	for _, pre := range res.CommonPrefixes {
		items := strings.Split(pre, "/")
		jobname := items[len(items)-2]
		if names[jobname] {
			log.Infoln(pre, "Active job")
		} else {
			err = cl.clearS3(pre)
			if err != nil {
				log.Infoln(err)
				return
			}
		}
	}
}

//clearS3 all files with specified prefix
func (cl *Client) clearS3(prefix string) error {
	log.Infoln("Deleting S3", prefix)
	res, err := cl.bucket.List(prefix, "", "", 1000)
	if err != nil {
		return err
	}
	log.Info(len(res.Contents))
	var wg sync.WaitGroup
	ch := make(chan string, 15)
	//Start 10 worker goroutinues to delete in parallel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range ch {
				err := cl.bucket.Del(key)
				if err != nil {
					log.Warn(err)
				}
			}
		}()
	}
	for _, item := range res.Contents {
		ch <- item.Key
	}
	close(ch)
	wg.Wait()
	if res.IsTruncated {
		//Restart if there is more...
		return cl.clearS3(prefix)
	}
	return nil
}

//Deploy allocates kubernetes objects
func (cl *Client) Deploy(jb *MapReduceJob) error {
	//Aquire a lock
	updateobj := jsonpatch.New()
	updateobj = updateobj.Add("test", "/status", StatusPending)
	updateobj = updateobj.Add("replace", "/status", StatusDeploying)
	err := cl.PatchJob(jb.Name, jb.Namespace, updateobj)
	if err != nil {
		//We failed to aquire the lock. Perhaps another operater is working on it
		return err
	}
	log.Infof("Aquired lock for %s/%s", jb.Namespace, jb.Name)
	//We now hold the lock. Fingers cross everything goes fine...
	//Take the pod template from jobspec and stamp kubemr specific things onto it

	podspec := *jb.Spec.spec.Template
	podspec.Spec.RestartPolicy = v1.RestartPolicyOnFailure

	podenv := []v1.EnvVar{
		//Name of job to run
		v1.EnvVar{
			Name:  "KUBEMR_JOB_NAME",
			Value: jb.Name,
		},
		//Namespace of job. Perhaps we can use downward api
		v1.EnvVar{
			Name:  "KUBEMR_JOB_NAMESPACE",
			Value: jb.Namespace,
		},
		//S3 region for intermediate files
		v1.EnvVar{
			Name: "KUBEMR_S3_REGION",
			ValueFrom: &v1.EnvVarSource{
				ConfigMapKeyRef: &v1.ConfigMapKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: "kubemr"},
					Key:                  "s3region",
				},
			},
		},
		//S3 region for intermediate files
		v1.EnvVar{
			Name: "KUBEMR_S3_ENDPOINT",
			ValueFrom: &v1.EnvVarSource{
				ConfigMapKeyRef: &v1.ConfigMapKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: "kubemr"},
					Key:                  "s3endpoint",
				},
			},
		},
		//S3 bucket name for intermediate files
		v1.EnvVar{
			Name: "KUBEMR_S3_BUCKET_NAME",
			ValueFrom: &v1.EnvVarSource{
				ConfigMapKeyRef: &v1.ConfigMapKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: "kubemr"},
					Key:                  "bucketname",
				},
			},
		},
		//S3 key prefix for intermediate files
		v1.EnvVar{
			Name: "KUBEMR_S3_BUCKET_PREFIX",
			ValueFrom: &v1.EnvVarSource{
				ConfigMapKeyRef: &v1.ConfigMapKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: "kubemr"},
					Key:                  "bucketprefix",
				},
			},
		},
		//S3 access id for intermediate files
		v1.EnvVar{
			Name: "KUBEMR_S3_ACCESS_KEY_ID",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: "kubemr"},
					Key:                  "S3_ACCESS_KEY_ID",
				},
			},
		},
		//S3 access secret for intermediate files
		v1.EnvVar{
			Name: "KUBEMR_S3_SECRET_ACCESS_KEY",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{Name: "kubemr"},
					Key:                  "S3_SECRET_ACCESS_KEY",
				},
			},
		},
	}

	//Stamp our env to each container, unsure which one holds the job...
	for i := range podspec.Spec.Containers {
		podspec.Spec.Containers[i].Env = append(podspec.Spec.Containers[i].Env, podenv...)
	}

	jobspec := batchv1.Job{
		//Metadata
		ObjectMeta: metav1.ObjectMeta{
			Name:      jb.Name,
			Namespace: jb.Namespace,
		},
		//Jobspec
		Spec: batchv1.JobSpec{
			Parallelism: jb.Spec.spec.Replicas,
			//Pod template
			Template: podspec,
		},
	}

	//Create kubemr secret in ns for this job
	err = cl.EnsureSecretExists("kubemr", jb.Namespace)
	if err != nil {
		log.Error(err)
		updateobj = jsonpatch.New()
		updateobj = updateobj.Add("replace", "/status", StatusFail)
		updateobj = updateobj.Add("replace", "/err", err.Error())
		return cl.PatchJob(jb.Name, jb.Namespace, updateobj)
	}
	//Already mounted secret into job container
	//Create kubemr configmap in ns for this job
	err = cl.EnsureConfigMapExists("kubemr", jb.Namespace)
	if err != nil {
		log.Error(err)
		updateobj = jsonpatch.New()
		updateobj = updateobj.Add("replace", "/status", StatusFail)
		updateobj = updateobj.Add("replace", "/err", err.Error())
		return cl.PatchJob(jb.Name, jb.Namespace, updateobj)
	}
	//Already mounted configmap into job container
	//Create job object in ns
	err = cl.DeployJob(&jobspec)
	if err != nil {
		log.Error(err)
		updateobj = jsonpatch.New()
		updateobj = updateobj.Add("replace", "/status", StatusFail)
		updateobj = updateobj.Add("replace", "/err", err.Error())
		return cl.PatchJob(jb.Name, jb.Namespace, updateobj)
	}
	log.Info(jobspec)
	//Reached so far... means all good...
	//Update the job status
	return cl.PatchJob(jb.Name, jb.Namespace, jsonpatch.New().Add("replace", "/status", StatusDeployed))
}

//DeployJob deploys the batch job
func (cl *Client) DeployJob(jobspec *batchv1.Job) error {
	_, err := cl.clientset.BatchV1().Jobs(jobspec.Namespace).Create(jobspec)
	return err
}

// EnsureConfigMapExists ensures config map exists
func (cl *Client) EnsureConfigMapExists(name, ns string) error {
	config := cl.config.Map()
	cfg, err := cl.clientset.CoreV1().ConfigMaps(ns).Get(name, metav1.GetOptions{})
	if err == nil {
		if reflect.DeepEqual(config, cfg.Data) {
			log.Info("configmap is fine...")
			return nil
		}
		log.Info("Need to replace configmap...")
		cfg.Data = config
		return cl.ReplaceConfigMap(cfg)
	}
	return cl.CreateConfigMap(name, ns, config)
}

//ReplaceConfigMap replaces a configmap
func (cl *Client) ReplaceConfigMap(cfg *v1.ConfigMap) error {
	_, err := cl.clientset.CoreV1().ConfigMaps(cfg.Namespace).Update(cfg)
	return err
}

//CreateConfigMap ensures kubemr configmap is present in the namespace
func (cl *Client) CreateConfigMap(name, ns string, config map[string]string) error {
	cfg := &v1.ConfigMap{
		Data: config,
	}
	cfg.Name = name
	_, err := cl.clientset.CoreV1().ConfigMaps(ns).Create(cfg)
	return err
}

// EnsureSecretExists ensures the kubemr secret exists
func (cl *Client) EnsureSecretExists(name, ns string) error {
	//Prepare secret payload
	secret := make(map[string][]byte)
	for k, v := range cl.secrets {
		secret[k] = []byte(v)
	}
	s, err := cl.clientset.CoreV1().Secrets(ns).Get(name, metav1.GetOptions{})
	if err == nil {
		if reflect.DeepEqual(secret, s.Data) {
			log.Info("secret is fine...")
			return nil
		}
		log.Info("Need to replace secret...")
		s.Data = secret
		return cl.ReplaceSecret(s)
	}
	return cl.CreateSecret(name, ns, secret)
}

//ReplaceSecret replaces a secret
func (cl *Client) ReplaceSecret(s *v1.Secret) error {
	_, err := cl.clientset.CoreV1().Secrets(s.Namespace).Update(s)
	return err
}

//CreateSecret ensures kubemr secret is present in the namespace
func (cl *Client) CreateSecret(name, ns string, secret map[string][]byte) error {
	s := &v1.Secret{
		Data: secret,
	}
	s.Type = "Opaque"
	s.Name = name
	_, err := cl.clientset.CoreV1().Secrets(ns).Create(s)
	return err
}

//CheckSecret checks if secret is present
func (cl *Client) CheckSecret(name, ns string) error {
	//TODO
	_, err := cl.clientset.CoreV1().Secrets(ns).Get(name, metav1.GetOptions{})
	return err
}

//GetSecretData gets plaintext secret data
func (cl *Client) GetSecretData(name, ns string) (map[string]string, error) {
	//TODO
	s, err := cl.clientset.CoreV1().Secrets(ns).Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	data := s.Data
	//data is base64'd, we must decode it
	out := make(map[string]string)
	for k, v := range data {
		out[k] = string(v)
	}
	return out, nil
}

//PatchJob applies an atomic json patch to the job
func (cl *Client) PatchJob(name, ns string, patchObj jsonpatch.Patch) error {
	//Need to make a new ResourceClient based on the ns of the object
	log.Info(patchObj)
	rescl := cl.tprclient.Resource(jobResource, ns)
	b, err := json.Marshal(patchObj)
	if err != nil {
		return err
	}
	log.Info(string(b))
	res, err := rescl.Patch(name, types.JSONPatchType, b)
	if err != nil {
		log.Info(res)
		return err
	}
	return nil
}
