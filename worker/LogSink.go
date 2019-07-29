package worker

import (
	"context"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"gocrontab/src/github.com/projectyk/ykcrontab/common"
	"time"
)

// mongodb存储日志

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch // 当前的批次
		commitTimer  *time.Timer      //
		timeoutBatch *common.LogBatch // 超时批次
	)

	for {
		select {
		case log = <-logSink.logChan:
			// 把log写入到mongodb中
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 让这个批次超时自动提交
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						// 发出超时通知,不要直接提交batch
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}

			// 把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			// 如果批次满了，就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				// 发送日志
				logSink.saveLogs(logBatch)

				// 清空
				logBatch = nil

				// 取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-logSink.autoCommitChan: // 过期的批次
			// 判断过期批次是否扔旧是当前批次
			if timeoutBatch != logBatch {
				continue // 跳过已经提交的批次
			}
			// 把批次写入
			logSink.saveLogs(timeoutBatch)
			logBatch = nil

		}
	}
}

// 初始化日志
func InitLogSink() (err error) {
	var (
		client *mongo.Client
	)

	// 建立mongodb链接
	if client, err = mongo.Connect(
		context.TODO(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)); err != nil {
		return
	}

	// 选择db和collection
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个mongodb处理协程
	go G_logSink.writeLoop()

	return
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	select {
	case logSink.logChan <- jobLog:
	default:
		// 队列满了就丢弃

	}

}
