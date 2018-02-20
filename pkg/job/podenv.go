package job

import "k8s.io/api/core/v1"

func stampCommonEnv(cfg *Config) []v1.EnvVar {
	return []v1.EnvVar{
		//Name of job to run
		v1.EnvVar{
			Name:  "KUBEMR_JOB_URL",
			Value: cfg.JobURL,
		},
		//S3 region for intermediate files
		v1.EnvVar{
			Name:  "KUBEMR_S3_REGION",
			Value: cfg.S3Region,
		},
		//S3 region for intermediate files
		v1.EnvVar{
			Name:  "KUBEMR_S3_ENDPOINT",
			Value: cfg.S3Endpoint,
		},
		//S3 bucket name for intermediate files
		v1.EnvVar{
			Name:  "KUBEMR_S3_BUCKET_NAME",
			Value: cfg.BucketName,
		},
		//S3 key prefix for intermediate files
		v1.EnvVar{
			Name:  "KUBEMR_S3_BUCKET_PREFIX",
			Value: cfg.BucketPrefix,
		},
	}
}
