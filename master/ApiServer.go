package master

import (
	"encoding/json"
	"fmt"
	"gocrontab/src/github.com/projectyk/ykcrontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	G_apiServer *ApiServer
)

// 保存任务接口
// POST job={"name":"job1","command":"echo hello","cronExpr":"*****"}
func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	// 任务保存到etcd中

	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)

	// 1 解析POST表单
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 2 取表单的job字段
	postJob = req.PostForm.Get("job")

	// 3 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	// 4 保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	// 5 返回正常应答({"errno":0,"msg","data":{...}})
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
	return

ERR:
	// 6 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
}

// 删除任务接口
//
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {

	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)

	//
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 删除的任务名
	name = req.PostForm.Get("name")

	// 去edct删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	// 5 返回正常应答({"errno":0,"msg","data":{...}})
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
	return

ERR:
	// 6 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}

}

// 列举所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		jobList []*common.Job
		err     error
		bytes   []byte
	)

	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}
	// 5 返回正常应答({"errno":0,"msg","data":{...}})
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
	return

ERR:
	// 6 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
}

// 杀死指定任务
// POST /job/killer/ name
func handleJobKill(resp http.ResponseWriter, req *http.Request) {

	var (
		err   error
		name  string
		bytes []byte
	)

	if err = req.ParseForm(); err != nil {
		return
	}

	// 获取要杀死的任务名
	name = req.PostForm.Get("name")

	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}

	// 5 返回正常应答({"errno":0,"msg","data":{...}})
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
	return

ERR:
	// 6 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
}

// 请求日志处理
func handleJobLog(resp http.ResponseWriter, req *http.Request) {

	var (
		err        error
		name       string
		skipParam  string // 从第几条开始
		limitParam string // 返回多少条
		skip       int
		limit      int
		bytes      []byte

		logArr []*common.JobLog
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	// 获取请求参数
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")

	if skip, err = strconv.Atoi(skipParam); err != nil {
		skip = 0
	}

	if limit, err = strconv.Atoi(limitParam); err != nil {
		limit = 20
	}

	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	// 5 返回正常应答({"errno":0,"msg","data":{...}})
	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
	return

ERR:
	// 6 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}

}

// 获取健康节点列表
func handleWorkerList(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	// 5 返回正常应答({"errno":0,"msg","data":{...}})
	if bytes, err = common.BuildResponse(0, "success", workerArr); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
	return

ERR:
	// 6 返回异常应答
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		fmt.Println(string(bytes))
		resp.Write(bytes)
	}
}

// 初始化服务
func InitApiServer() (err error) {

	var (
		mux           *http.ServeMux
		listener      net.Listener
		httpServer    *http.Server
		staticDir     http.Dir     // 静态文件根目录
		staticHandler http.Handler // 静态文件的HTTP回调
	)

	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 静态文件目录
	staticDir = http.Dir(G_config.Webroot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	// 启动TCP监听端口
	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	// 创建一个http服务
	httpServer = &http.Server{
		ReadHeaderTimeout: time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout:      time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:           mux,
	}

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动了服务端
	go httpServer.Serve(listener)

	return

}
