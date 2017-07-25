package worker

//JobWorker is the user implimentation that performs the map/reduce tasks
type JobWorker interface {
	Map(id int, input string, utils *Utilities) (outputs map[int]string, err error)
	Reduce(id int, inputs []string, utils *Utilities) (output string, err error)
}

//JobWorkerMerge is a JobWorker that can also do a ReduceMerge on results of Reduce to output single result
//TODO: Not actually being used - Future
type JobWorkerMerge interface {
	JobWorker
	ReduceMerge(inputs []string, args, secrets map[string]string, utils *Utilities) (output string, err error)
}
