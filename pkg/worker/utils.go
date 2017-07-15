package worker

import (
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/amz.v1/s3"
)

//Utilities provide common useful methods that map/reduce functions may make use off.
type Utilities struct {
	bucket *s3.Bucket
	prefix string
}

//NewUtilities creates new helper object
func NewUtilities(bucket *s3.Bucket, prefix string) *Utilities {
	return &Utilities{bucket: bucket, prefix: prefix}
}

//UploadFilename uploads file src into key in bucket
func (utils *Utilities) UploadFilename(key, src string) (string, error) {
	key = utils.prefix + key
	dst := "s3://" + utils.bucket.Name + key
	f, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return "", err
	}
	err = utils.bucket.PutReader(key, f, stat.Size(), "application/octet-stream", "")
	if err != nil {
		return "", err
	}
	return dst, nil
}

//GetS3Object gets object from s3, errors if src is not fully qualified uri matching our bucket
func (utils *Utilities) GetS3Object(src string) (io.ReadCloser, error) {
	if !strings.HasPrefix(src, "s3://"+utils.bucket.Name) {
		return nil, fmt.Errorf("src is not kubemr managed s3 resource belonging to this job")
	}
	key := strings.Replace(src, "s3://"+utils.bucket.Name, "", 1)
	return utils.bucket.GetReader(key)
}
