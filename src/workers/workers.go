package workers

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"strconv"
	"encoding/json"
	"context"
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
	i := 0
	for  {
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
		time.Sleep(time.Second * 3)
	}
}