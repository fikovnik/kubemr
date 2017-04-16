package worker

//JobWorker is the user implimentation that performs the map/reduce tasks
type JobWorker interface {
	Map() func(input string, args map[string]interface{}) (outputs []string, err error)
	Reduce() func(inputs []string, args map[string]interface{}) (output string, err error)
}
