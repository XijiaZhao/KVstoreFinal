package edu.case.kvstore.readonly.rpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QueryServiceClient {
    private final ManagedChannel channel;
    private final QueryServiceGrpc.QueryServiceBlockingStub blockingStub;

    private String host = "";
    private int port = -1;

    private Logger logger = LoggerFactory.getLogger(QueryServiceClient.class);

    /*public QueryServiceClient() {
        this("127.0.0.1", 50051);
    }*/

    public QueryServiceClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        this.host = host;
        this.port = port;
    }

    public QueryServiceClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = QueryServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdownNow();
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString(){
        return "QueryServiceClient: [host: " + this.host + ", port: " + this.port + "]";
    }

    //gRPC过程调用
    public RPCInterface.HelloReply sayHello(RPCInterface.HelloRequest request){
        RPCInterface.HelloReply helloReply;
        logger.info(toString());
        try{
            helloReply = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.toString());
            return null;
        }
        return helloReply;
    }

    //gRPC过程调用
    public RPCInterface.QueryReply get(byte[] key) throws IOException {
        RPCInterface.QueryKey queryKey = RPCInterface.QueryKey.newBuilder()
                .setKey(ByteString.copyFrom(key)).build();
        RPCInterface.QueryReply queryReply;
        try {
            queryReply = blockingStub.get(queryKey);
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed: {}", e.getStatus());
            logger.warn(e.toString());
            throw new IOException(e);
        }
        logger.info("RPC get return");
        return queryReply;
    }
}
