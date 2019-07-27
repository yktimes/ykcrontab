package main

import (
	"flag"
	"fmt"
	"gocrontab/src/github.com/projectyk/ykcrontab/master"
	"runtime"
	"time"
)

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	// master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

func main() {

	var (
		err error
	)

	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 启动API HTTP服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}
	// 正常退出

	for {
		time.Sleep(time.Second)
	}

	return

ERR:
	fmt.Println(err)
}
