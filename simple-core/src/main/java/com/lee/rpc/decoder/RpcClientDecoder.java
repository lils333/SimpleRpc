package com.lee.rpc.decoder;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcExceptionSerializer;
import com.lee.rpc.RpcMetadataSerializer;
import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.helper.RpcHelper;
import com.lee.rpc.helper.Weight;
import com.lee.rpc.helper.client.RpcServiceClientUnit;
import com.lee.rpc.helper.recycler.ByteBufInputStreamRecycler;
import com.lee.rpc.util.stream.ByteBufInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import com.lee.rpc.helper.client.ClientProxy;

import java.io.IOException;
import java.net.InetSocketAddress;

import static com.lee.rpc.helper.client.ClientHelper.CLIENT_GENERATOR;
import static com.lee.rpc.helper.client.ClientHelper.setValue;
import static com.lee.rpc.util.Constant.*;
import static com.lee.rpc.util.exception.ErrorType.CLIENT_SERIALIZER_ERROR;

/**
 * 客户端可以访问多个不同的RpcServer，所以这个地方需要根据serviceId来获取到底是哪个方法的，不像服务器端那样，只处理一种类型的RpcServer
 * <p>
 * 4          8          1         1     2            N bytes
 * serviceId  requestId  methodId  type  bodyLength   bodyContext
 * <p>
 *
 * @author l46li
 */
@Slf4j
public class RpcClientDecoder extends LengthFieldBasedFrameDecoder {

    private static final RpcExceptionSerializer RPC_EXCEPTION_SERIALIZER = new RpcExceptionSerializer();
    private static final RpcMetadataSerializer SERIALIZER = new RpcMetadataSerializer();
    protected ClientProxy clientProxy;
    protected InetSocketAddress address;

    public RpcClientDecoder() {
        this(1048576, true);
    }

    public RpcClientDecoder(InetSocketAddress address) {
        this(1048576, true);
        this.address = address;
    }

    public RpcClientDecoder(InetSocketAddress address, ClientProxy clientProxy) {
        this(1048576, true);
        this.address = address;
        this.clientProxy = clientProxy;
    }

    public RpcClientDecoder(int maxFrameLength, boolean failFast) {
        super(maxFrameLength, 14, 2, 0, 0, failFast);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        Object decode = super.decode(ctx, in);
        if (decode != null) {
            ByteBuf buffer = (ByteBuf) decode;
            try {
                int serviceId = buffer.readInt();
                long requestId = buffer.readLong();
                byte methodId = buffer.readByte();
                byte typeId = buffer.readByte();
                short bodyLength = buffer.readShort();

                switch (typeId) {
                    case NORMAL:
                        setValue(requestId,
                                unmarshallNormal(
                                        buffer, getRpcMethodUnit(serviceId, methodId), bodyLength, requestId
                                )
                        );
                        break;
                    case EMPTY_TYPE:
                        setValue(requestId, null);
                        break;
                    case ABNORMAL:
                        setValue(requestId, unmarshallAbnormal(buffer, bodyLength, requestId));
                        break;
                    case HEARTBEAT:
                        log.info("Received heartbeat pong message from server {}", serviceId);
                        break;
                    case METADATA:
                        //注意这个地方只是给local协议使用的，如果是zookeeper的话，那么是不走这个逻辑的
                        fillChannel(ctx, buffer, serviceId, bodyLength);
                        break;
                    default:
                        throw new RpcException("Can not process typeId " + typeId);
                }
            } finally {
                ReferenceCountUtil.release(buffer);
            }
        }
        return null;
    }

    private void fillChannel(ChannelHandlerContext ctx, ByteBuf buffer, int serviceId, short bodyLength) {
        handleMetadataRequest(buffer, bodyLength);

        ctx.pipeline().replace(
                "decoder", "decoder",
                CLIENT_GENERATOR.createDecoder(address, serviceId, clientProxy)
        );

        RpcServiceClientUnit rpcClientUnit = RpcHelper.getRpcClientUnit(serviceId);
        Weight weight = rpcClientUnit.getWeight(address);

        //这个地方按照weight,补齐剩下的没有创建的Channel
        for (int i = 1; i < weight.value(); i++) {
            clientProxy.createChannel(address);
        }
    }

    /**
     * 会被重写，所以这个地方的内容不重要
     *
     * @param serviceId 指定处理的serviceId
     * @param methodId  指定当前需要调用的方法id
     * @return 返回RpcMethodUnit
     */
    protected RpcMethodUnit getRpcMethodUnit(int serviceId, byte methodId) {
        return RpcHelper.getRpcClientUnit(serviceId).getMethodUnit(methodId);
    }

    private void handleMetadataRequest(ByteBuf buffer, short bodyLength) {
        try (ByteBufInputStream in = ByteBufInputStreamRecycler.newInstance(buffer)) {
            RpcHelper.registerClient(address, SERIALIZER.deserialize(in));
        } catch (IOException e) {
            //metadata 理论上面也不会出现错误的情况，因为是框架部分的序列化和饭序列化
            throw new RpcException("Can not read parameter from current com.lee.rpc buffer", e);
        }
    }

    private Object unmarshallNormal(ByteBuf buffer,
                                    RpcMethodUnit methodUnit,
                                    short bodyLength, long requestId) {
        try (ByteBufInputStream in = ByteBufInputStreamRecycler.newInstance(buffer)) {
            return methodUnit.deserializeToReturnType(in);
        } catch (Exception e) {
            //如果自定义反序列化错误，那么会发什么错误，这个时候，服务器端已经处理完成了，这个地方可以作为成功来处理
            RpcException exception = new RpcException().withStatus(CLIENT_SERIALIZER_ERROR).withError(e.getMessage());
            setValue(requestId, exception);
            throw exception;
        }
    }

    private RpcException unmarshallAbnormal(ByteBuf buffer, short bodyLength, long requestId) {
        try (ByteBufInputStream in = ByteBufInputStreamRecycler.newInstance(buffer)) {
            return RPC_EXCEPTION_SERIALIZER.deserialize(in);
        } catch (Exception e) {
            //理论上不会发生，错误代码是框架部分已经写好的，不存在其他情况
            RpcException exception = new RpcException().withStatus(CLIENT_SERIALIZER_ERROR).withError(e.getMessage());
            setValue(requestId, exception);
            throw exception;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Unexpected exception, ignore current response", cause);
    }
}
