package job

import (
	"fmt"
	"os"

	"gopkg.in/amz.v1/aws"
)

//Config holds values required for workers for kubemr internal things
//Not passed to user code
type Config struct {
	//one of https://godoc.org/gopkg.in/amz.v1/aws#pkg-variables ap-northeast-1, ap-southeast-1, etc...
	S3Region     string //The S3 region we wanna use for temporary stuff
	S3Endpoint   string //overrides region
	BucketName   string //A pre-existing bucket
	BucketPrefix string //Prepended to all keys, to reduce clutter in bucket root
	JobURL       string //The URL for job
}

//NewConfigEnv populates Config struct from env
func NewConfigEnv() *Config {
	return &Config{
		S3Region:     os.Getenv("KUBEMR_S3_REGION"),
		S3Endpoint:   os.Getenv("KUBEMR_S3_ENDPOINT"),
		BucketName:   os.Getenv("KUBEMR_S3_BUCKET_NAME"),
		BucketPrefix: os.Getenv("KUBEMR_S3_BUCKET_PREFIX"),
		JobURL:       os.Getenv("KUBEMR_JOB_URL"),
	}
}

//Validate validates the config
func (config *Config) Validate() error {
	_, ok := aws.Regions[config.S3Region]
	if !ok {
		return fmt.Errorf("Region %s is invalid", config.S3Region)
	}
	if config.BucketName == "" {
		return fmt.Errorf("BucketName must be provided")
	}
	return nil
}

//Map converts config to data item for configmap
func (config *Config) Map() map[string]string {
	return map[string]string{
		"s3region":     config.S3Region,
		"bucketname":   config.BucketName,
		"bucketprefix": config.BucketPrefix,
		"s3endpoint":   config.S3Endpoint,
	}
}
