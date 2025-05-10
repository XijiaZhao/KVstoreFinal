package edu.case.kvstore.readonly.master;

import edu.case.kvstore.readonly.rpc.MasterServiceGrpc;
import edu.case.kvstore.readonly.rpc.RPCInterface;
import edu.case.kvstore.readonly.rpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Master {
    private Server server;
    private int port;
    private int WORKER_MAXNUM;
    private Logger logger;

    private MetaInfoManager metaInfoManager;

    //private Master(){}

    public Master(int port, int workerMaxNum) {
        this.port = port;
        this.WORKER_MAXNUM = workerMaxNum;
    }

    private void start() throws IOException {
        //创建元信息管理器MetaInfoManager
        metaInfoManager = new MetaInfoManager(WORKER_MAXNUM);
        //主进程启动MasterService
        this.server = ServerBuilder.forPort(port)
                .addService(new MasterService())
                .build()
                .start();
        logger = LoggerFactory.getLogger(Master.class);
        logger.info("Server started , listening on {}, WORKER_MAXNUM: {}", port, WORKER_MAXNUM);
        /*Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("shutting down gRPC server since JVM is shutting down");
            Master.this.stop();
            System.err.println("server shut down");
            logger.info("shut down ");
        }));*/
        //主进程启动完成，等待各个Worker调用MasterService的RPC接口
    }

    public void stop() {
        if (server != null) {
            //关闭MasterService
            server.shutdown();
            //关闭MetaInfoManager
            metaInfoManager.stopTimerAndClearInfos();
        }
    }

    // block 一直到退出程序
    void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final Master server = new Master(50051, 3);
        server.start();
        server.blockUntilShutdown();
    }

    public static Master startMaster(int aPort, int aWorkerMaxNum) {
        final Master server = new Master(aPort, aWorkerMaxNum);
        new Thread(()-> {
            try {
                server.start();
                server.blockUntilShutdown();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        return server;
    }

    private class MasterService extends MasterServiceGrpc.MasterServiceImplBase {
        @Override
        public void reportHeartbeat(RPCInterface.WorkerInfoState req,
                                    StreamObserver<RPCInterface.MasterReply> responseObserver) {
            //给Worker添加时间戳，管理交给MataInfoManager
            WorkerInfo worker = new WorkerInfo(req, System.currentTimeMillis());
            RPCInterface.MasterReply reply = metaInfoManager.updateWorkerState(worker).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
