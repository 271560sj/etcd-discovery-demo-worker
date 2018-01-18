package main

import "./workers"
func main()  {
	endPoints := []string{"http://192.168.133.130:2379","http://192.168.133.131:2379","http://192.168.133.132:2379"}
	worker := workers.InitWorker("salers", endPoints)
	worker.WriterInfosBeat()
}
