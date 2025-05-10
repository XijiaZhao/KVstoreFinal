package edu.case.kvstore.readonly.master;

import edu.case.kvstore.readonly.rpc.RPCInterface;
import edu.case.kvstore.readonly.rpc.WorkerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//使用包作用域
class MetaInfoManager {
    final static int HEARTBEAT_TIMEOUT_IN_MILLISECOND = 12000;
    private final int WORKER_MAXNUM;
    private final Logger logger = LoggerFactory.getLogger(MetaInfoManager.class);
    //这里先用一个int记录数量，未来或许可以维护两张表，分别记录已分配和未分配的Worker
    private WorkerInfo[] workerInfos;
    private RPCInterface.MasterReply.MasterState masterState;
    private TimerThread timer;
    private int CUR_WORKER_NUM = 0;

    //MetaInfoManager构造方法
    public MetaInfoManager(int workerMaxNum) {
        this.WORKER_MAXNUM = workerMaxNum;
        //初始化Worker信息数组 WorkerInfo workers[]
        this.workerInfos = new WorkerInfo[WORKER_MAXNUM];
        for (int i = 0; i < workerInfos.length; i++) {
            workerInfos[i] = new WorkerInfo();
        }
        //初始化timer
        this.timer = new TimerThread();
        this.timer.setDaemon(true);
        this.timer.start();
        //初始化MetaInfoManager状态（将State设置为INIT)
        this.masterState = RPCInterface.MasterReply.MasterState.INIT;
    }

    public RPCInterface.MasterReply.MasterState getMasterState() {
        return masterState;
    }

    public void setMasterState(RPCInterface.MasterReply.MasterState masterState) {
        this.masterState = masterState;
    }

    //结束MetaInfoManager
    public void stopTimerAndClearInfos() {
        timer.exit = true;
        try {
            timer.join();
        } catch (InterruptedException e) {
            logger.warn("Exception from {}", Thread.currentThread().getName());
            e.printStackTrace();
        }
        workerInfos = null;
    }

    //更新列表，由上层Master类调用，先保留着，新的方法如果遇到错误可以用来参考

    /*public synchronized RPCInterface.MasterReply.Builder updateWorkerState(WorkerInfo worker) {
        RPCInterface.MasterReply.Builder reply = RPCInterface.MasterReply.newBuilder();
        int allocatedID = -1;
        //case 1: masterState == INIT
        if (masterState == MasterState.INIT) {
            //case 1.1: 存在没有被分配的Worker --> 添加至列表，并更新服务器状态
            if (CUR_WORKER_NUM < WORKER_MAXNUM) {
                allocatedID = addToWorkerInfos(worker);
                if (CUR_WORKER_NUM == WORKER_MAXNUM) {
                    masterState = MasterState.WORKING;
                }
            }
            //case 1.2: 所有Worker均已经被分配 --> 仅log.Waring
            else if (CUR_WORKER_NUM == WORKER_MAXNUM) {
                logger.error("case 1.2: all workers have been allocated \n MasterState: {} WorkerInfo: {}\n",
                        masterState.toString(), worker.toString());
            }
        }
        //case 2: masterState == WORKING
        else if (masterState == MasterState.WORKING) {
            //case 2.1: 是新的Worker --> 根据列表情况处理
            if (!worker.isWorked()) {
                //case 2.1.1: 存在没有被分配的Worker --> 添加至列表
                if (CUR_WORKER_NUM < WORKER_MAXNUM) {
                    allocatedID = addToWorkerInfos(worker);
                }
                //case 2.1.2: 所有Worker均已经被分配 --> 仅log.Waring
                else if (CUR_WORKER_NUM == WORKER_MAXNUM) {
                    logger.warn("case 2.1.2: all workers have been allocated \n MasterState: {} WorkerInfo: {}\n",
                            masterState.toString(), worker.toString());
                }
            }
            //case 2.2: 是工作过的Worker --> 检查UUID
            else {
                //case 2.2.1 UUID不一致 --> 这个Worker是断了重连，而且坑已经被占了，弃之不用，输出Warning
                if (!workerInfos[worker.getWorkerID()].getWorkerUUID().equals(worker.getWorkerUUID())) {
                    logger.warn("case 2.2.1: UUID不一致 \n MasterState: {} WorkerInfo: {}\n",
                            masterState.toString(), worker.toString());
                }
                //case 2.2.2 UUID一致 --> 续命成功，保持ID不变，更新lastHeartbeatTimestamp 虽然我不是什么蛤丝但偷偷蛤一下应该不会有人发现的
                else {
                    allocatedID = worker.getWorkerID();
                    workerInfos[worker.getWorkerID()].setLastHeartbeatTimestamp(worker.getLastHeartbeatTimestamp());
                }
            }
        }
        //将MetaInfoManager管理的Worker状态依次添加********这个地方可不可以优化下？？？
        for (WorkerInfo aWorkerInfo : workerInfos) {
            reply.addWorkers(aWorkerInfo.toWorkerInfoState());
        }
        return reply.setWorkerID(allocatedID)
                .setMasterState(masterState);
    }*/

    /*public synchronized RPCInterface.MasterReply.Builder updateWorkerState(WorkerInfo worker) {
        RPCInterface.MasterReply.Builder reply = RPCInterface.MasterReply.newBuilder();
        int allocatedID = -1;
        //列表未满
        if (CUR_WORKER_NUM < WORKER_MAXNUM) {
            //是工作过的ID并且UUID一致-->仅更新时间戳
            if (worker.getWorkerID() >= 0 &&
                    workerInfos[worker.getWorkerID()].getWorkerUUID().equals(worker.getWorkerUUID())) {
                allocatedID = worker.getWorkerID();
                workerInfos[worker.getWorkerID()].setLastHeartbeatTimestamp(worker.getLastHeartbeatTimestamp());
            } else {
                allocatedID = addToWorkerInfos(worker);
                if (CUR_WORKER_NUM == WORKER_MAXNUM && getMasterState() == MasterState.INIT) {
                    setMasterState(MasterState.WORKING);
                }
            }
        } else {
            if (getMasterState() == MasterState.WORKING) {
                logger.warn("all workers have been allocated, \n MasterState: {} WorkerInfo: {}\n",
                        masterState.toString(), worker.toString());
            } else if (getMasterState() == MasterState.INIT) {
                workerInfos[worker.getWorkerID()].setLastHeartbeatTimestamp(worker.getLastHeartbeatTimestamp());
                logger.error("all workers have been allocated! \n MasterState: {} WorkerInfo: {}\n",
                        masterState.toString(), worker.toString());
            }
        }
        //将MetaInfoManager管理的Worker状态依次添加********这个地方可不可以优化下？？？
        for (WorkerInfo aWorkerInfo : workerInfos) {
            reply.addWorkers(aWorkerInfo.toWorkerInfoState());
        }
        return reply.setWorkerID(allocatedID)
                .setMasterState(masterState);
    }*/

    public synchronized RPCInterface.MasterReply.Builder updateWorkerState(WorkerInfo worker) {
        int allocatedID = -1;
        boolean isListChanged = false;
        //面向Worker状态的流程
        //新的Worker，并且不是用户查询用的Worker
        if (worker.getWorkerUUID().equals("LeaveForUserToQuery")) {
            return buildMasterReply(allocatedID);
        }

        if (worker.getWorkerID() == -1) {
            if (CUR_WORKER_NUM > WORKER_MAXNUM) {
                logger.error("CUR_WORKER_NUM > WORKER_MAXNUM");
            }
            //列表未满 --> 加入列表
            if (CUR_WORKER_NUM < WORKER_MAXNUM) {
                allocatedID = addToWorkerInfos(worker);
                isListChanged = true;
            }
            //列表已满 所有Worker均已经被分配 --> 仅log.Waring
            else {
                allocatedID = -1;
                logger.warn("all workers have been allocated \n MasterState: {} WorkerInfo: {}\n",
                        masterState.toString(), worker.toString());
            }
        }
        //工作过的Worker
        else {
            //UUID一致 --> 更新时间戳
            if (workerInfos[worker.getWorkerID()].getWorkerUUID().equals(worker.getWorkerUUID())) {
                allocatedID = worker.getWorkerID();
                workerInfos[worker.getWorkerID()].setLastHeartbeatTimestamp(worker.getLastHeartbeatTimestamp());
                if (workerInfos[worker.getWorkerID()].getQueryServicePort() != worker.getQueryServicePort()) {
                    workerInfos[worker.getWorkerID()].setQueryServicePort(worker.getQueryServicePort());
                    isListChanged = true;
                }
            }
            //UUID不一致
            else {
                //列表未满 --> 加入列表
                if (CUR_WORKER_NUM < WORKER_MAXNUM) {
                    allocatedID = addToWorkerInfos(worker);
                    isListChanged = true;
                }
                //列表已满 --> 仅log.Waring
                else {
                    allocatedID = -1;
                    logger.warn("all workers have been allocated, \n MasterState: {} WorkerInfo: {}\n",
                            masterState.toString(), worker.toString());
                }
            }
        }

        //更新MasterState
        if (getMasterState() == RPCInterface.MasterReply.MasterState.INIT && CUR_WORKER_NUM == WORKER_MAXNUM) {
            setMasterState(RPCInterface.MasterReply.MasterState.WORKING);
            isListChanged = true;
        }

        if (isListChanged) {
            logger.info(WorkerListToString());
        }

        return buildMasterReply(allocatedID);
    }

    private RPCInterface.MasterReply.Builder buildMasterReply(int allocatedID) {
        RPCInterface.MasterReply.Builder reply = RPCInterface.MasterReply.newBuilder();
        //将MetaInfoManager管理的Worker状态依次添加
        for (WorkerInfo aWorkerInfo : workerInfos) {
            reply.addWorkers(aWorkerInfo.toWorkerInfoState());
        }
        return reply.setWorkerID(allocatedID)
                .setMasterState(masterState);
    }

    //添加一个worker到列表中
    private int addToWorkerInfos(WorkerInfo worker) {
        //找到最小的没有被分配的worker
        int i;
        boolean is_addToList = false;
        for (i = 0; i < WORKER_MAXNUM; i++) {
            if (workerInfos[i].getWorkerID() < 0) {
                //先加1再分配比较合适
                CUR_WORKER_NUM++;
                workerInfos[i] = worker; //将其分配
                workerInfos[i].setWorkerID(i);
                is_addToList = true;
                break;
            }
        }
        if (is_addToList) {
            logger.info("allocate ID: {} to workerUUID: {} \n WorkerInfo: {}\n", i, worker.getWorkerUUID(), worker.toString());
            return i;
        } else {
            logger.warn("fail to addlocate ID to wokerUUID: {}, all workers in list are working", worker.getWorkerUUID());
            return -1;
        }
    }

    //检查Worker列表，剔除过时的，由Timer调用
    private synchronized void checkWorkerInfo() {
        long curTimer = System.currentTimeMillis();
        for (WorkerInfo workerInfo : workerInfos) {
            assert (curTimer > workerInfo.getLastHeartbeatTimestamp());
            if (workerInfo.isWorked() &&
                    (curTimer - workerInfo.getLastHeartbeatTimestamp() > HEARTBEAT_TIMEOUT_IN_MILLISECOND)) {
                logger.warn("OverTimed Worker has been cleared\n{}\n", workerInfo.toString());
                //先Clear再减1比较合适
                workerInfo.clear();
                logger.info(WorkerListToString());
                CUR_WORKER_NUM--;
            }
        }
    }

    private String WorkerListToString() {
        StringBuilder strbder = new StringBuilder("WorkerInfo:\n");
        for (WorkerInfo aWorkerInfo : workerInfos) {
            strbder.append(aWorkerInfo.toString()).append("\n");
        }
        return strbder.toString();
    }

    class TimerThread extends Thread {
        public volatile boolean exit = false;
        private final int SLEEP_TIME_IN_MILLISECOND = 3000;

        @Override
        public void run() {
            while (!exit) {
                //logger.info("Timer running...");
                checkWorkerInfo();
                try {
                    Thread.sleep(SLEEP_TIME_IN_MILLISECOND);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /*enum MasterServiceState {
        INIT(1, "INIT"), WORKING(2, "WORKING");
        public final int stateValue;
        private final String str;

        private MasterServiceState(int stateValue, String str) {
            this.stateValue = stateValue;
            this.str = str;
        }

        @Override
        public String toString() {
            return this.str;
        }

        //MasterState由protocbuf编译器构造
        //在MetaInfoManager里是由MasterService类的实例保存Master的状态
        //故需要在gRPC调用时将MaterServiceState转为MasterState
        public RPCInterface.MasterReply.MasterState toMasterState(MasterServiceState masterServiceState){
            if(masterServiceState == INIT) {
                return RPCInterface.MasterReply.MasterState.INIT;
            }else if(masterServiceState == WORKING){
                return RPCInterface.MasterReply.MasterState.WORKING;
            }
            return RPCInterface.MasterReply.MasterState.UNRECOGNIZED;
        }
    }*/
}