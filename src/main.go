package main

import "./workers"
func main()  {
	endPoints := []string{"http://127.0.0.1:2379"}
	worker := workers.InitWorker("salers","127.0.0.1",endPoints)
	worker.WriterInfosBeat()
}
