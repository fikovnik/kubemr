package job

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/nbari/violetear"
)

func (jb *MapReduceJob) handleGet(w http.ResponseWriter, r *http.Request) {
	jb.RLock()
	defer jb.RUnlock()
	j, err := json.Marshal(jb)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-type", "application/json")
	w.Write(j)
	jb.poke <- true
}

func (jb *MapReduceJob) handleMap(w http.ResponseWriter, r *http.Request) {
	jb.Lock()
	defer jb.Unlock()
	defer r.Body.Close()
	//Abort if we are not in map phase
	if jb.Status != StatusMap {
		http.Error(w, fmt.Sprintf("Not in map phase"), http.StatusBadRequest)
		return
	}
	taskidStr := violetear.GetParam("taskid", r)
	taskid, err := strconv.Atoi(taskidStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	decoder := json.NewDecoder(r.Body)
	task := MapTask{}
	err = decoder.Decode(&task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//Store the object only if there is no worker assigned, or the worker is the owner
	obj, found := jb.Maps[taskid]
	if !found {
		http.Error(w, fmt.Sprintf("Task %v is not found", taskid), http.StatusNotFound)
		return
	}
	if obj.Worker != "" && obj.Worker != task.Worker {
		//Task exists, and worker is different..
		//Unsure... bad request or forbidden?
		http.Error(w, fmt.Sprintf("Task %v is already aquired by %s", taskid, obj.Worker), http.StatusBadRequest)
		return
	}
	//ok... all good so far...
	jb.Maps[taskid] = task
	jb.poke <- true
}

func (jb *MapReduceJob) handleReduce(w http.ResponseWriter, r *http.Request) {
	jb.Lock()
	defer jb.Unlock()
	defer r.Body.Close()
	//Abort if we are not in reduce phase
	if jb.Status != StatusReduce {
		http.Error(w, fmt.Sprintf("Not in reduce phase"), http.StatusBadRequest)
		return
	}
	taskidStr := violetear.GetParam("taskid", r)
	taskid, err := strconv.Atoi(taskidStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	decoder := json.NewDecoder(r.Body)
	task := ReduceTask{}
	err = decoder.Decode(&task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//Store the object only if there is no worker assigned, or the worker is the owner
	obj, found := jb.Reduces[taskid]
	if !found {
		http.Error(w, fmt.Sprintf("Task %v is not found", taskid), http.StatusNotFound)
		return
	}
	if obj.Worker != "" && obj.Worker != task.Worker {
		//Task exists, and worker is different..
		//Unsure... bad request or forbidden?
		http.Error(w, fmt.Sprintf("Task %v is already aquired by %s", taskid, obj.Worker), http.StatusBadRequest)
		return
	}
	//ok... all good so far...
	jb.Reduces[taskid] = task
	jb.poke <- true
}
