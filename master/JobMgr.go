package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"gocrontab/src/github.com/projectyk/ykcrontab/common"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 得到KV和lease的子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	fmt.Println(kv)

	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}

// 保存任务
func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 把任务保存到 /cron/jobs/任务名 —> json

	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)

	// etcd保存key
	jobKey = common.JOB_SAVE_DIR + job.Name

	if jobValue, err = json.Marshal(job); err != nil {
		return
	}

	// 保存到etcd
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}

	// 如果是更新,返回旧值
	if putResp.PrevKv != nil {
		// 对旧值做一个反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 删除任务

func (jobMgr *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {

	var (
		jobKey    string
		delResp   *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	// etcd中保存任务的key
	jobKey = common.JOB_SAVE_DIR + name

	if delResp, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	// 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		// 旧值解析
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}

		oldJob = &oldJobObj
	}

	return
}

// etcd列举所有任务
func (jobMgr *JobMgr) ListJobs() (jobList []*common.Job, err error) {

	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)

	// etcd中保存任务的key
	dirKey = common.JOB_SAVE_DIR

	// 获取目录下所有的任务信息
	if getResp, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化切片空间
	jobList = make([]*common.Job, 0) // 这样我们可以判断空间大小是否为0，而不用判断是否是空指针，降低复杂性

	// 遍历所有任务,进行反序列化
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}

		jobList = append(jobList, job)
	}

	return
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) (err error) {

	// 更新一下key=/cron/killer/任务名
	var (
		killerKey      string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
	)

	// 通知worker杀死对应任务
	killerKey = common.JOB_KILLER_DIR + name

	// 让ｗｏｒｋｅｒ监听到一次put操作,创建一个租约让其稍后自动过期

	if leaseGrantResp, err = jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	// 租约ＩＤ
	leaseId = leaseGrantResp.ID

	// 设置killer标记
	if _, err = jobMgr.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return
}
