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

type Worker struct {
	Name string
	IP string
	KeysAPI etcd.KeysAPI
}
type WorkInfo struct {
	IDs string
	KeyWord string
	Infos string
}

func InitWorker(name,ip string,endpoints []string)  *Worker{
	cfg := etcd.Config{
		Endpoints: endpoints,
		Transport: etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * 5,
	}

	client,err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Error:connecting to etcd error",err)
	}

	worker := &Worker{
		Name: name,
		IP: ip,
		KeysAPI: etcd.NewKeysAPI(client),
	}

	return worker
}

func (w *Worker)WriterInfosBeat()  {
	keysApi := w.KeysAPI
	i := 1
	for  {
		fmt.Printf("Send book infos of %d \n",i)
		sendBookInfos(i,keysApi)
		if i % 2 == 0 {
			fmt.Printf("Change book infos of %d \n",i)
			changeBookInfos(i,keysApi)
		}
		if i % 3 ==0 {
			fmt.Printf("Delete book infos of %d \n",i)
			deleteBookInfos(i,keysApi)
		}
		i ++
		if i > 30 {
			break
		}
		time.Sleep(time.Second * 3)
	}
}

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