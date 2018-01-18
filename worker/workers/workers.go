package workers

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"encoding/json"
	"context"
	"runtime"
	"os"
)

//设置worker service的结构体
type Worker struct {
	Name string
	KeysAPI etcd.KeysAPI
}
//设置记录消息的结构体
type ServiceInfo struct {
	IP string
	CPU int
	HostName string
	ServiceName string
	ServiceIP string
	ServicePort string
}

//初始化worker service连接etcd
func InitWorker(name string,endpoints []string)  *Worker{
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
		KeysAPI: etcd.NewKeysAPI(client),
	}

	return worker
}

//向etcd写入信息
func (w *Worker)WriterInfosBeat()  {

	//注册服务信息
	svInfo := registryServiceInfos(w)
	time.Sleep(time.Second * 10)
	//更新服务信息
	updateServiceInfos(w,svInfo)
	time.Sleep(time.Second * 10)
	//删除服务信息
	deleteServiceInfos(w,svInfo)
	time.Sleep(time.Second * 10)
}
//注册服务信息
func registryServiceInfos(w *Worker) *ServiceInfo{
	//获取主机名
	hostName,_ := os.Hostname()
	svInfo := &ServiceInfo{
		IP: "127.0.0.1",
		CPU: runtime.NumCPU(),
		HostName: hostName,
		ServiceIP: "127.0.0.1",
		ServicePort: "8090",
		ServiceName: w.Name,
	}

	keys := "service-info"
	value,_ := json.Marshal(svInfo)
	result,err := w.KeysAPI.Set(context.Background(),keys,string(value),&etcd.SetOptions{
		TTL: time.Second * 10,
	})

	if err != nil {
		log.Fatal("Error registry service infos",err)
	}else {
		log.Println("Registry service info success ",result.Node.Value)
	}
	return svInfo
}
//更新服务信息
func updateServiceInfos(w *Worker, service *ServiceInfo)  {
	service.ServiceName = "updateService"
	service.ServicePort = "3012"

	keys := "service-info"
	value,_ := json.Marshal(service)

	result,err := w.KeysAPI.Update(context.Background(),keys,string(value))

	if err != nil {
		log.Fatal("Error update service info",err)
	}else {
		log.Println("Update service infos success ",result.Node.Value)
	}
}
//删除服务信息
func deleteServiceInfos(w *Worker,service *ServiceInfo)  {
	keys := "service-info"
	result,err := w.KeysAPI.Delete(context.Background(),keys,&etcd.DeleteOptions{
		Recursive: true,
	})
	if err != nil {
		log.Fatal("Error delete service infos",err)
	}else {
		log.Println("Delete service infos success ",result.Node.Value)
	}
}