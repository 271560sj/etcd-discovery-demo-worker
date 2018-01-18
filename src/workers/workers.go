package workers

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"strconv"
	"encoding/json"
	"context"
	"fmt"
)

//设置worker service的结构体
type Worker struct {
	Name string
	IP string
	KeysAPI etcd.KeysAPI
}
//设置记录消息的结构体
type WorkInfo struct {
	IDs string
	KeyWord string
	Infos string
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

	return worker
}

//向etcd写入信息
func (w *Worker)WriterInfosBeat()  {
	//获取操作key的API
	keysApi := w.KeysAPI
	i := 1
	for  {//循环写入key的信息
		fmt.Printf("Send book infos of %d \n",i)
		//创建key,并写入value值
		sendBookInfos(i,keysApi)
		if i % 2 == 0 {
			fmt.Printf("Change book infos of %d \n",i)
			//修改key的value值
			changeBookInfos(i,keysApi)
		}
		if i % 3 ==0 {
			fmt.Printf("Delete book infos of %d \n",i)
			//删除key
			deleteBookInfos(i,keysApi)
		}
		i ++
		if i > 30 {
			break
		}
		time.Sleep(time.Second * 3)
	}
}
//创建key,并写入value值
func sendBookInfos(i int,keysApi etcd.KeysAPI)  {
	ids := "books" + strconv.Itoa(i)
	keyWords := "book"
	infos := "it is a book about num of " + strconv.Itoa(i)
	i ++
	wkInfos := &WorkInfo{
		IDs: ids,
		KeyWord: keyWords,
		Infos: infos,
	}
	key := "books/" + ids
	value,_ := json.Marshal(wkInfos)
	_,err := keysApi.Set(context.Background(),key,string(value),&etcd.SetOptions{
		TTL: time.Second * 10,
	})
	if err != nil {
		log.Println("Error send book infos",err)
	}
	log.Println("Send book infos success,",string(value))
}
//删除key
func deleteBookInfos(i int,keysApi etcd.KeysAPI)  {
	ids := "books" + strconv.Itoa(i)
	keys := "books/" + ids
	value,err := keysApi.Delete(context.Background(),keys,nil)
	if err != nil {
		log.Println("Error delete keys,",err)
	}else {
		log.Println("Deletes keys success",value)
	}
}
//修改key的value值
func changeBookInfos(i int, keysApi etcd.KeysAPI )  {
	ids := "books" + strconv.Itoa(i)
	keys := "books/" + ids
	wkInfos := &WorkInfo{
		IDs: ids,
		KeyWord: "book",
		Infos: "update the book's infos",
	}
	values,_ := json.Marshal(wkInfos)
	value,err := keysApi.Set(context.Background(),keys,string(values),nil)
	if err != nil {
		log.Println("Error update book's infos",err)
	}else {
		log.Println("Success update book's infos",value)
	}
}