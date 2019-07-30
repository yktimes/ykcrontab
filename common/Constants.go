package common

const (
	// 保存任务目录
	JOB_SAVE_DIR   = "/cron/jobs/"
	JOB_KILLER_DIR = "/cron/killer/"

	// 保存任务事件
	JOB_EVENT_SAVE = 1
	// 删除任务事件
	JOB_EVENT_DELETE = 2

	// 任务锁目录
	JOB_LOCK_DIR = "/cron/lock/"

	JOB_EVENT_KILL = 3

	// 服务注册目录
	JOB_WORKER_DIR = "/cron/workers/"
)
