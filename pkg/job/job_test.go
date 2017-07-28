package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/phayes/freeport"

	"k8s.io/client-go/kubernetes/fake"
)

func makejob(t *testing.T) *MapReduceJob {
	j := `
  {
    "name": "foo",
    "inputs": ["a", "b", "c"],
    "template": {
      "spec": {
        "volumes":[{
          "name":"tmpdir",
          "emptyDir": {}
        }],
        "containers": [{
          "name": "wordcount",
          "image": "turbobytes/kubemr-wordcount",
          "volumeMounts": [{
            "name": "tmpdir",
            "mountPath": "/tmp"
          }]
        }]
      }
    }
  }
  `
	jb := &MapReduceJob{}
	err := json.Unmarshal([]byte(j), jb)
	if err != nil {
		t.Error(err)
	}
	return jb
}

func gethttp(method, url, body string, t *testing.T) int {
	var req *http.Request
	var err error
	if method == http.MethodGet {
		req, err = http.NewRequest(method, url, nil)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		req, err = http.NewRequest(method, url, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
	}
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode
}

//Test normal workflow...
func TestMRJobFlowOK(t *testing.T) {
	cl := fake.NewSimpleClientset()
	addr := fmt.Sprintf(":%v", freeport.GetPort())
	t.Log(addr)
	jb := makejob(t)
	err := jb.Init(cl, addr, "127.0.0.1", &Config{})
	if err != nil {
		t.Error(err)
	}
	errch := make(chan error)
	go func() {
		//TODO: Need better way than stupid sleep...
		time.Sleep(time.Millisecond * 30)
		baseurl := fmt.Sprintf("http://127.0.0.1%s/%s/%s/", addr, jb.Name, jb.uuid)
		t.Log(baseurl)
		if gethttp(http.MethodGet, baseurl, "", t) != 200 {
			errch <- fmt.Errorf("%s returned status not 200", baseurl)
		}
		if gethttp(http.MethodPut, baseurl+"map/0", `{"worker":"foo","input":"a","outputs":{"1":"a","2":"b"},"error":"","status":"COMPLETE"}`, t) != 200 {
			errch <- fmt.Errorf("%s returned status not 200", baseurl+"map/0")
		}
		if gethttp(http.MethodPut, baseurl+"map/1", `{"worker":"foo","input":"a","outputs":{"1":"c","2":"d"},"error":"","status":"COMPLETE"}`, t) != 200 {
			errch <- fmt.Errorf("%s returned status not 200", baseurl+"map/1")
		}
		if gethttp(http.MethodPut, baseurl+"map/2", `{"worker":"foo","input":"a","outputs":{"1":"e","2":"f"},"error":"","status":"COMPLETE"}`, t) != 200 {
			errch <- fmt.Errorf("%s returned status not 200", baseurl+"map/2")
		}
		time.Sleep(time.Millisecond * 30)
		//Ensure map stage is now reduce...
		if jb.Status != StatusReduce {
			errch <- fmt.Errorf("Expected reduce stage, got %s", jb.Status)
		}
		if len(jb.Reduces) != 2 {
			errch <- fmt.Errorf("Expected 2 reduce tasks, got %v", len(jb.Reduces))
		}
		//Do the reduce tasks...
		if gethttp(http.MethodPut, baseurl+"reduce/1", `{"worker":"foo","inputs":["a","c","e"],"output":"foo","error":"","status":"COMPLETE"}`, t) != 200 {
			errch <- fmt.Errorf("%s returned status not 200", baseurl+"reduce/1")
		}
		if gethttp(http.MethodPut, baseurl+"reduce/2", `{"worker":"foo","inputs":["a","c","e"],"output":"bar","error":"","status":"COMPLETE"}`, t) != 200 {
			errch <- fmt.Errorf("%s returned status not 200", baseurl+"reduce/2")
		}
		time.Sleep(time.Millisecond * 30)
		//Check if completed...
		if jb.Status != StatusComplete {
			errch <- fmt.Errorf("Expected completed stage, got %s", jb.Status)
		}
		if jb.Results[0] != "foo" {
			errch <- fmt.Errorf("First result should be foo, got %s", jb.Results[0])
		}
		if jb.Results[1] != "bar" {
			errch <- fmt.Errorf("Second result should be bar, got %s", jb.Results[1])
		}

	}()
	go func() {
		errch <- jb.Start(time.Minute)
		close(errch)
	}()
	for err := range errch {
		if err != nil {
			t.Fatal(err)
		}
	}
	//Check if secrets exist...
}
