package common

import (
	"context"
	"encoding/json"
	"gocrontab/src/github.com/gorhill/cronexpr"
	"strings"
	"time"
)

// 定时任务
type Job struct {
	Name     string `json:"name"`     // 任务名
	Command  string `json:"command"`  // shell命令
	CronExpr string `json:"cronExpr"` // cron表达式
}

// HTTP接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

// 应答方法
func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	// 1 定义一个response
	var (
		response Response
	)

	response.Errno = errno
	response.Msg = msg
	response.Data = data

	// 2 序列化
	resp, err = json.Marshal(response)

	return
}

// 反序列化job

func UnpcakJob(value []byte) (ret *Job, err error) {
	var (
		job *Job
	)

	job = &Job{}

	if err = json.Unmarshal(value, job); err != nil {
		return
	}

	ret = job
	return

}

// 从etcd的key提取任务名
// /cron/jobs/job10 去掉/cron/jobs/
func ExtractJobName(jobKey string) string {

	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// 任务变化结构体
type JobEvent struct {
	EventType int //save delete
	Job       *Job
}

// 任务变化事件有两种，一个是更新，另一个是删除
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

//任务调度计划
type JobSchedulePlan struct {
	Job      *Job                 // 要调度的任务信息
	Expr     *cronexpr.Expression // cronexpr 表达式
	NextTime time.Time            // 下次调度时间
}

// 构造执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {

	var (
		expr *cronexpr.Expression
	)

	// 解析JOB的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		return
	}

	// 生成调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}

	return
}

// 任务执行状态
type JobExecuteInfo struct {
	Job      *Job      // 任务信息
	PlanTime time.Time // 理论上的调度时间
	RealTime time.Time // 实际调度事件

	CancelCtx  context.Context    // 任务Command 的context
	CancelFunc context.CancelFunc //  用于取消command执行的cancel函数
}

// 构造执行状态信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {

	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(),
	}

	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo // 执行状态
	Output      []byte          // 脚本输出
	Err         error           // 脚本错误原因
	StartTime   time.Time       // 启动时间
	EndTime     time.Time       // 结束时间
}

// 从etcd的key提取任务名
// /cron/killer/job10 去掉/cron/jobs/
func ExtractKillerName(killerKey string) string {

	return strings.TrimPrefix(killerKey, JOB_KILLER_DIR)
}
