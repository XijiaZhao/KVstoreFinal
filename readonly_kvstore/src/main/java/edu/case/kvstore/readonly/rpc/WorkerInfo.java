package edu.case.kvstore.readonly.rpc;

import java.sql.Timestamp;

public class WorkerInfo {
    private int workerID = -1;
    private String workerUUID = "";
    private String hostName = "";
    private int queryServicePort = -1;
    private long lastHeartbeatTimestamp = -1;

    //构造方法
    public WorkerInfo() {
    }

    public WorkerInfo(RPCInterface.WorkerInfoState workerInfoState, long lastHeartbeatTimestamp) {
        this.workerID = workerInfoState.getMyWorkerID();
        this.workerUUID = workerInfoState.getWorkerUUID();
        this.hostName = workerInfoState.getHostName();
        this.queryServicePort = workerInfoState.getQueryServicePort();
        this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
    }

    public void setFromWorkerInfoState(RPCInterface.WorkerInfoState worker){
        this.workerID = worker.getMyWorkerID();
        this.workerUUID = worker.getWorkerUUID();
        this.hostName = worker.getHostName();
        this.queryServicePort = worker.getQueryServicePort();
        this.lastHeartbeatTimestamp = -1;
    }

    public void copyFromWorkerInfo(WorkerInfo workerInfo){
        this.setWorkerID(workerInfo.getWorkerID());
        this.setWorkerUUID(workerInfo.getWorkerUUID());
        this.setHostName(workerInfo.getHostName());
        this.setQueryServicePort(workerInfo.getQueryServicePort());
        this.lastHeartbeatTimestamp = -1;
    }

    //检查worker的位置是工作过的吗　工作中　--> true
    public boolean isWorked() {
        assert (workerID >= 0 || workerID == -1);
        return (this.workerID >= 0);
    }

    //简单清空信息
    public void clear() {
        this.workerID = -1;
        this.workerUUID = "";
        this.hostName = "";
        this.queryServicePort = -1;
        this.lastHeartbeatTimestamp = -1;
    }

    public int getWorkerID() {
        return workerID;
    }

    public void setWorkerID(int workerID) {
        this.workerID = workerID;
    }

    public String getWorkerUUID() {
        return workerUUID;
    }

    public void setWorkerUUID(String workerUUID) {
        this.workerUUID = workerUUID;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getQueryServicePort() {
        return queryServicePort;
    }

    public void setQueryServicePort(int queryServicePort) {
        this.queryServicePort = queryServicePort;
    }

    public long getLastHeartbeatTimestamp() {
        return lastHeartbeatTimestamp;
    }

    public void setLastHeartbeatTimestamp(long lastHeartbeatTimestamp) {
        this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
    }

    @Override
    public String toString() {
        return "WorkerInfo:[WorkerID: " + workerID +
                ", WorkerUUID: " + workerUUID +
                ", hostName: " + hostName +
                ", queryServicePort: " + queryServicePort +
                ", lastHeartBeatTimeStamp: " + new Timestamp(lastHeartbeatTimestamp) + "]";
    }

    public RPCInterface.WorkerInfoState toWorkerInfoState() {
        return RPCInterface.WorkerInfoState.newBuilder()
                .setMyWorkerID(workerID)
                .setWorkerUUID(workerUUID)
                .setHostName(hostName)
                .setQueryServicePort(queryServicePort)
                .build();
    }

    //弃用
    /*
    public WorkerInfo toWorkerInfo(RPCInterface.WorkerInfoState){
        WorkerInfo workerInfo = new WorkerInfo();
        workerInfo.setLastHeartbeatTimestamp();
    }

    //Worker在接受Master回复时需要将RPCInterface.WorkerInfoState[] 转为本地的WorkerState[]
    //有无更好的处理手段？***********************************************************？
    //不能简单的将两个类合并为一个类：WorkerInfoState中没有时间戳;也不能使用继承，WorkerInfoStated定义为fianl
    public static WorkerInfo[] toWorkerInfos(List<RPCInterface.WorkerInfoState> list){
        int size = list.size();
        WorkerInfo[] workerInfos = new WorkerInfo[size];
        for(int i = 0; i < size; i++){
            workerInfos[i] = list[i]
        }
    }*/
}