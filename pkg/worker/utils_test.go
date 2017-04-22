package worker

import (
	"io/ioutil"
	"os"
	"testing"

	"gopkg.in/amz.v1/aws"
	"gopkg.in/amz.v1/s3"
	"gopkg.in/amz.v1/s3/s3test"
)

func TestUtilities(t *testing.T) {
	srv, err := s3test.NewServer(&s3test.Config{})
	if err != nil {
		t.Error(err)
	}
	defer srv.Quit()
	region := aws.Region{
		Name:                 "test",
		S3Endpoint:           srv.URL(),
		S3LocationConstraint: true, // s3test server requires a LocationConstraint
		Sign:                 aws.SignV2,
	}
	auth := aws.Auth{}
	s := s3.New(auth, region)
	bucket := s.Bucket("foo")
	err = bucket.PutBucket(s3.Private)
	if err != nil {
		t.Error(err)
	}
	utils := NewUtilities(bucket, "/foo/")
	//Create tmp file
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Error(err)
	}
	n := f.Name()
	f.WriteString("foobar")
	f.Close()
	defer os.Remove(n)
	//Write to the tmp file
	loc, err := utils.UploadFilename("key", n)
	if err != nil {
		t.Error(err)
	}
	if loc != "s3://foo/foo/key" {
		t.Errorf("Expected location s3://foo/foo/key, instead got %s", loc)
	}
	item, err := utils.GetS3Object("s3://foo/foo/key")
	if err != nil {
		t.Error(err)
	}
	d, err := ioutil.ReadAll(item)
	if err != nil {
		t.Error(err)
	}
	defer item.Close()
	if string(d) != "foobar" {
		t.Errorf("Expected contents foobar, instead got %s", d)
	}
	//Ask for something not managed
	item, err = utils.GetS3Object("s3://foos/foo/key")
	if err == nil {
		t.Error("Expected an error, got nil")
	}
}
