package worker

import (
	"fmt"
	"gocrontab/src/github.com/projectyk/ykcrontab/common"
	"time"
)

// 任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent              // etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo  // 任务执行表
	jobResultChan     chan *common.JobExecuteResult      // 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理任务事件
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {

	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted      bool
		err             error
	)

	switch jobEvent.EventType {
	case common.JOB_EVENT_SAVE: //保存任务事件

		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		// 加入计划表
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan

	case common.JOB_EVENT_DELETE: //任务删除事件

		// 判断是否计划表存在该key
		if jobSchedulePlan, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			// 从计划表删除
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}
	}
}

// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {

	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)

	// 调度和执行是两件事情

	// 执行的任务可能运行很久，1分钟会调度60次，但是只能执行1次,防止并发

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("尚未退出,跳过执行:", jobPlan.Job.Name)
		return
	}

	//构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)
	//保存任务状态

	fmt.Println("执行任务:", jobExecuteInfo.Job.Name, jobExecuteInfo.RealTime)
	// 执行任务
	G_executor.ExecuteJob(jobExecuteInfo)

}

// 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {

	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)

	// 如果任务表为空，随便睡眠
	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = time.Second
		return
	}

	// 当前时间
	now = time.Now()

	// 1 遍历所有任务
	for _, jobPlan = range scheduler.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			// TODO 尝试执行任务
			scheduler.TryStartJob(jobPlan)
			fmt.Printf("执行任务 %s\n：", jobPlan.Job.Name)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间

		}

		// 统计最近一个要过期的任务时间
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}
	// 下次调度时间(最近要执行的任务调度事件-当前时间)
	scheduleAfter = (*nearTime).Sub(now)

	return
}

// 处理任务结果

func (scheduler *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	// 删除执行任务
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)
	fmt.Println("任务执行完成：", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
}

// 调度协程
func (scheduler *Scheduler) scheduleLoop() {

	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 初始化一次（1s）
	scheduleAfter = scheduler.TrySchedule()

	// 调度的延时定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务 common.Job
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: // 监听任务变化时间
			// 对内存中的维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // 最近的任务到期了

		case jobResult = <-scheduler.jobResultChan: //监听任务执行结果
			scheduler.handleJobResult(jobResult)
		}
		// 调度一次任务
		scheduleAfter = scheduler.TrySchedule()

		//重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}

// 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}

	// 启动调度协程
	go G_scheduler.scheduleLoop()

	return
}

// 回传任务结果
func (scheduler *Scheduler) PushJobResult(result *common.JobExecuteResult) {
	scheduler.jobResultChan <- result
}
