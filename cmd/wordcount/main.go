package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/onrik/logrus/filename"
	log "github.com/sirupsen/logrus"
	"github.com/turbobytes/kubemr/pkg/worker"
)

type myWorker struct{}

//Using FNV-1a non-cryptographic hash function to determine partition for particular key
func hash(s string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	hash := h.Sum32() % uint32(n)
	return int(hash)
}

//Map for wordcount treats input as HTTP URL and outputs S3 object URIs as result
//Tokenize words :-
//word1, 1
//word2, 1
//and so on...
func (w myWorker) Map(id int, input string, utils *worker.Utilities) (outputs map[int]string, err error) {
	outputs = make(map[int]string)
	log.Info("Running map on ", input)
	//Create one TempFile for each partition
	tmpfiles := make([]*os.File, 5) //Hardcoded here, could be dynamic, or passed thru args
	for i := range tmpfiles {
		tmpfiles[i], err = ioutil.TempFile("", "")
		if err != nil {
			return outputs, err
		}
	}
	//Fetch the input url
	resp, err := http.Get(input)
	if err != nil {
		return outputs, err
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Split(bufio.ScanWords)
	//Map each instance of a word with 1, use FNV hash to write it to its corresponding partition file
	for scanner.Scan() {
		word := scanner.Text()
		//TODO: Maybe make everything lowercase... and check if its really a "word"
		partition := hash(word, 5)
		fmt.Fprintf(tmpfiles[partition], "%s\t1\n", word)
	}
	//Close each TempFile and upload to S3
	for i, f := range tmpfiles {
		f.Close()
		newpath, err := utils.UploadFilename(fmt.Sprintf("map/%v-%v.txt", id, i), f.Name())
		if err != nil {
			return outputs, err
		}
		outputs[i] = newpath
	}
	//Return the list of S3 files
	return outputs, nil
}

//Reduce for wordcount treats inputs as S3 object URI and outputs a S3 object URI as result
//Merge all inputs
//Sort it
//Output results with counts :-
//word1, 102
//word2, 55
//and so on...
func (w myWorker) Reduce(id int, inputs []string, utils *worker.Utilities) (string, error) {
	f, err := ioutil.TempFile("", "")
	//Store filename for future use
	fname := f.Name()
	if err != nil {
		return "", err
	}
	var rd io.ReadCloser
	//Download and merge each input into local file
	for _, input := range inputs {
		rd, err = utils.GetS3Object(input)
		if err != nil {
			return "", err
		}
		_, err = io.Copy(f, rd)
		if err != nil {
			return "", err
		}
		rd.Close()
	}
	f.Close()
	cmd := exec.Command("sort", fname)
	sorted, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	cmd.Stdout = sorted
	err = cmd.Run()
	if err != nil {
		return "", err
	}
	sortedfname := sorted.Name()
	sorted.Close()
	log.Info("sorted", sortedfname)
	os.Remove(fname)
	output, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	f, err = os.Open(sortedfname)
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	word := ""
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		splitted := strings.Split(line, "\t")
		if splitted[0] != word {
			//New word detected, yield previous word
			if count > 0 {
				fmt.Fprintf(output, "%s\t%d\n", word, count)
			}
			word = splitted[0]
			count, err = strconv.Atoi(splitted[1])
			if err != nil {
				return "", err
			}
		} else {
			count++
		}
	}
	//Yield last word
	if count > 0 {
		fmt.Fprintf(output, "%s\t%d\n", word, count)
	}
	f.Close()
	os.Remove(sortedfname)
	outname := output.Name()
	output.Close()

	return utils.UploadFilename(fmt.Sprintf("reduce/%v.txt", id), outname)
}

func init() {
	//log.SetFormatter(&log.JSONFormatter{})
	filenameHook := filename.NewHook()
	log.AddHook(filenameHook)
}

func main() {
	runner, err := worker.NewRunner()
	if err != nil {
		log.Error(err)
		return //Silent fail
	}
	log.Info(runner)

	err = runner.Run(myWorker{})
	if err != nil {
		log.Error(err)
	}
	//Terminate successfully to let k8s clear this pod
}
