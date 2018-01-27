package workers

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"strconv"
	"encoding/json"
	"context"
)

const (
	//记录关键字
	workerKey = "workerService"
	watcherKey = "masterService"
	ip = "192.168.133.132"
	port = "2365"
	name = "worker"

)

//设置worker service的结构体
type Worker struct {
	Name string
	IP string
	KeysAPI etcd.KeysAPI
}
//设置记录消息的结构体
type WorkInfo struct {
	IP string
	Port string
	Name string
}

//初始化worker service连接etcd
func InitWorker(name,ip string,endpoints []string)  *Worker{
	cfg := etcd.Config{//设置初始化service时，需要的参数
		//Endpoints数组用于记录etcd各个节点的信息，基本格式为[]string{"http://ip:port",.....}
		//Transport设置数据传输协议，如果不设置，默认的方式是DefaultTransport
		//HeaderTimeoutPerRequest设置每次请求的超时时间
		Endpoints: endpoints,
		Transport: etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * 5,
	}

	//创建连接etcd
	client,err := etcd.New(cfg)
	if err != nil {//判断连接失败
		log.Fatal("Error:connecting to etcd error",err)
	}

	//设置worker service的信息
	worker := &Worker{
		Name: name,
		IP: ip,
		KeysAPI: etcd.NewKeysAPI(client),
	}

	go worker.WorkerService()

	return worker
}

//向etcd写入信息
func (w *Worker)WorkerService()  {
	//获取操作key的API
	keysApi := w.KeysAPI
	i := 1
	for  {//循环写入key的信息
		//创建key,并写入value值
		go sendWorkerInfos(i,keysApi)
		go changeWorkerService(i,keysApi)
		go deleteWorkerService(keysApi)
		go watcherMasterService(keysApi)
		i ++
		if i > 30 {
			break
		}
		time.Sleep(time.Second * 3)
	}
}
//创建key,并写入value值
func sendWorkerInfos(i int,keysApi etcd.KeysAPI)  {
	//获取worker 编号
	ids := strconv.Itoa(i)
	//设置work service的关键关键字
	wkInfos := &WorkInfo{
		IP: ip,
		Port: port,
		Name: name + ids,
	}
	value,_ := json.Marshal(wkInfos)
	response,err := keysApi.Set(context.Background(),workerKey,string(value),&etcd.SetOptions{
	})
	if err != nil {
		log.Println("Error send worker service infos",err)
	}else {
		dealWithData(response)
	}
}
//删除key
func deleteWorkerService(keysApi etcd.KeysAPI)  {
	value,err := keysApi.Delete(context.Background(),workerKey,nil)
	if err != nil {
		log.Println("Error delete keys,",err)
	}else {
		dealWithData(value)
	}
}
//修改key的value值
func changeWorkerService(i int, keysApi etcd.KeysAPI )  {
	ids := strconv.Itoa(i)
	wkInfos := &WorkInfo{
		IP: ip,
		Port: port + ",changed",
		Name: name + ids,
	}
	values,_ := json.Marshal(wkInfos)
	value,err := keysApi.Set(context.Background(),workerKey,string(values),nil)
	if err != nil {
		log.Println("Error update worker service infos",err)
	}else {
		dealWithData(value)
	}
}

//处理获取的数据
func dealWithData(nodes *etcd.Response)  {
	node := nodes.Node
	key := node.Key
	value := node.Value
	ttl := strconv.FormatInt(node.TTL,10)
	log.Print(nodes.Action + "," + key + "," + value + "," + ttl)
}

//监控Master的注册信息
func watcherMasterService(keysApi etcd.KeysAPI)  {
	keyApi := keysApi
	watcher := keyApi.Watcher(watcherKey,&etcd.WatcherOptions{
		Recursive:true,
	})

	for{
		nodes, err := watcher.Next(context.Background())
		if err != nil {
			 log.Println("Watcher master service error")
			 continue
		}
		dealWithData(nodes)
	}
}