package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

//Client uses a jobs http API
type Client struct {
	baseurl string
	client  *http.Client
}

//NewClient creates new job client
func NewClient(baseurl string) *Client {
	return &Client{
		baseurl: baseurl,
		client:  &http.Client{Timeout: time.Second},
	}
}

//GetJob gets the job at this baseurl
func (cl *Client) GetJob() (*MapReduceJob, error) {
	resp, err := cl.client.Get(cl.baseurl)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Got status: %s", resp.Status)
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	jb := &MapReduceJob{}
	err = decoder.Decode(jb)
	return jb, err
}

func (cl *Client) put(url string, payload []byte) (bool, error) {
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	if err != nil {
		return false, err
	}
	resp, err := cl.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil //Success! job aquired
	case http.StatusBadRequest:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		log.Info(string(b))
		return false, nil //Failure to aquire, but dont err...
	default:
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("%s: %s", resp.Status, string(b))
	}
}

//PutMap puts MapTask
func (cl *Client) PutMap(task MapTask, taskid int) (bool, error) {
	payload, err := json.Marshal(task)
	if err != nil {
		return false, err
	}
	url := fmt.Sprintf("%smap/%v/", cl.baseurl, taskid)
	return cl.put(url, payload)
}

//PutReduce puts ReduceTask
func (cl *Client) PutReduce(task ReduceTask, taskid int) (bool, error) {
	payload, err := json.Marshal(task)
	if err != nil {
		return false, err
	}
	url := fmt.Sprintf("%sreduce/%v/", cl.baseurl, taskid)
	return cl.put(url, payload)
}
