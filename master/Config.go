package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

var (
	// 单例
	G_config *Config
)

// 程序配置
type Config struct {
	ApiPort         int      `json:"apiPort"`
	ApiReadTimeout  int      `json:"apiReadTimeout"`
	ApiWriteTimeout int      `json:"apiWriteTimeout"`
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
	Webroot         string   `json:"webroot"`

	MongodbUri            string `json:"mongodbUri"`
	MongodbConnectTimeout int    `json:"mongodbConnectTimeout"`
}

// 加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)
	// 把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2 json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {

		return
	}
	fmt.Println(conf)
	// 3 赋值单例
	G_config = &conf

	return
}
