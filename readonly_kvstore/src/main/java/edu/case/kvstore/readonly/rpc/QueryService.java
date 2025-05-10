package edu.case.kvstore.readonly.rpc;

import edu.case.kvstore.readonly.worker.bucketstorage.BucketStorage;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class QueryService {
    private Server server;
    private int port;

    private BucketStorage bucketStorage;
    private Logger logger = LoggerFactory.getLogger(QueryService.class);

    public QueryService(int port, BucketStorage bucketStorage) throws IOException {
        this.port = port;
        this.bucketStorage = bucketStorage;
        start();
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new QueryServiceImpl())
                .build()
                .start();
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    class QueryServiceImpl extends QueryServiceGrpc.QueryServiceImplBase {
        @Override
        public void sayHello(RPCInterface.HelloRequest request,
                             StreamObserver<RPCInterface.HelloReply> responseObserver) {
            RPCInterface.HelloReply reply = RPCInterface.HelloReply.newBuilder()
                    .setPort(port)
                    .build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void get(RPCInterface.QueryKey request,
                        StreamObserver<RPCInterface.QueryReply> responseObserver) {
            RPCInterface.QueryReply.Builder queryReply = RPCInterface.QueryReply.newBuilder();
            byte[] value = null;
            try {
                value = bucketStorage.get(request.getKey().toByteArray());
            } catch (IOException e) {
                //不设置value
                logger.warn(e.toString());
                queryReply.setErrno(RPCInterface.QueryReply.Errno.ERROR);
                responseObserver.onNext(queryReply.build());
                responseObserver.onCompleted();
                return;
            }

            if (value == null) {
                //未查询到key
                queryReply.setErrno(RPCInterface.QueryReply.Errno.NULL);
            } else {
                queryReply.setErrno(RPCInterface.QueryReply.Errno.OK);
                queryReply.setVal(ByteString.copyFrom(value));
            }
            responseObserver.onNext(queryReply.build());
            responseObserver.onCompleted();
        }
    }
}
