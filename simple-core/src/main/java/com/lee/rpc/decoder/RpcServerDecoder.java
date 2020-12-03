package com.lee.rpc.decoder;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcMetadataSerializer;
import com.lee.rpc.RpcMethodUnit;
import com.lee.rpc.RpcRequest;
import com.lee.rpc.helper.RpcHelper;
import com.lee.rpc.helper.recycler.ByteBufInputStreamRecycler;
import com.lee.rpc.helper.recycler.ByteBufOutputStreamRecycler;
import com.lee.rpc.helper.recycler.RpcRequestRecycler;
import com.lee.rpc.helper.server.RpcServiceServerUnit;
import com.lee.rpc.util.stream.ByteBufInputStream;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import static com.lee.rpc.util.Constant.*;
import static com.lee.rpc.util.exception.ErrorType.*;

/**
 * 实际上大部分时候RPC调用的参数都只有一个，所以这个地方可以先置使用一个参数
 * 2            8           1          1               2            2                  N bytes               .....
 * serviceId    requestId   methodId   parameterNum    bodyLength   parameterLength     parameterBodyLength   .....
 * <p>
 * <p>
 * 实际情况如下:
 * 4         8           1          1      2            N bytes
 * serverId  requestId   methodId   type   bodyLength   bodyContent
 *
 * @author l46li
 */
@Slf4j
public class RpcServerDecoder extends LengthFieldBasedFrameDecoder {

    private static final RpcMetadataSerializer RPC_METADATA_SERIALIZER = new RpcMetadataSerializer();

    public RpcServerDecoder() {
        this(1048576, true);
    }

    public RpcServerDecoder(int maxFrameLength, boolean failFast) {
        super(maxFrameLength, 14, 2, 0, 0, failFast);
    }

    @Override
    protected Object decode(final ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        Object decode = super.decode(ctx, in);
        if (decode != null) {
            ByteBuf byteBuf = (ByteBuf) decode;
            try {
                final int serviceId = byteBuf.readInt();
                final long requestId = byteBuf.readLong();
                final byte methodId = byteBuf.readByte();
                final byte typeId = byteBuf.readByte();
                final short bodyLength = byteBuf.readShort();

                switch (typeId) {
                    case OBJECT:
                        RpcMethodUnit methodUnit = getRpcMethodUnit(serviceId, methodId);
                        RpcRequest request = RpcRequestRecycler.newInstance(serviceId, requestId, methodId);
                        methodUnit.getExecutor().execute(
                                request.channel(ctx.channel())
                                        .withMethodUnit(methodUnit)
                                        .type(typeId)
                                        .body(unmarshall(byteBuf, bodyLength, request))
                        );
                        break;
                    case EMPTY_TYPE:
                        methodUnit = getRpcMethodUnit(serviceId, methodId);
                        request = RpcRequestRecycler.newInstance(serviceId, requestId, methodId);
                        methodUnit.getExecutor().execute(
                                request.channel(ctx.channel())
                                        .withMethodUnit(methodUnit)
                                        .type(typeId)
                                        .body(EMPTY_VALUE)
                        );
                        break;
                    case HEARTBEAT:
                        ctx.executor().execute(() -> ctx.writeAndFlush(
                                handleHeartbeatRequest(
                                        ctx, requestId, methodId, typeId, serviceId
                                ))
                        );
                        break;
                    case METADATA:
                        ctx.executor().execute(() -> ctx.writeAndFlush(
                                handleMetadataRequest(
                                        ctx, requestId, methodId, typeId, serviceId
                                )
                        ));
                        break;
                    default:
                        throw new RpcException()
                                .withStatus(NOT_SUPPORT_TYPE)
                                .withError("Can not support type " + typeId)
                                .withRequest(
                                        RpcRequestRecycler.newInstance(serviceId, requestId, methodId)
                                );

                }
            } finally {
                //一定要释放，不管任何原因，都需要释放掉
                ReferenceCountUtil.release(byteBuf);
            }
        }

        //不做任何处理，要么数据不够，要么已经处理了
        return null;
    }

    /**
     * 使用 javassist 重写了该方法，主要是为了不需要在去RpcHelper里面去获取RpcMethodUnit，而是直接生成一个子类
     * 然后子类把RpcMethodUnit作为字段存放，根据传递进来的methodId，直接获取到对应的RpcMethodUnit字段
     *
     * @param methodId 传递进来需要的methodId
     * @return 返回对应的一个RpcMethodUnit
     */
    protected RpcMethodUnit getRpcMethodUnit(int serviceId, byte methodId) {
        return RpcHelper.getRpcServiceUnit(serviceId).getMethodUnit(methodId);
    }

    private Object handleHeartbeatRequest(ChannelHandlerContext ctx,
                                          long requestId, byte methodId, byte typeId, int serviceId) {
        ByteBuf byteBuf = ctx.alloc().ioBuffer();
        byteBuf.writeInt(serviceId);
        byteBuf.writeLong(requestId);
        byteBuf.writeByte(methodId);
        byteBuf.writeByte(typeId);
        byteBuf.writeShort(EMPTY_VALUE);
        return byteBuf;
    }

    private Object handleMetadataRequest(ChannelHandlerContext ctx,
                                         long requestId, byte methodId, byte typeId, int serviceId) {
        RpcServiceServerUnit rpcServiceUnit = RpcHelper.getRpcServiceUnit(serviceId);
        if (rpcServiceUnit == null) {
            throw new RpcException()
                    .withStatus(NOT_EXIST_SERVICE_ID)
                    .withError("Can not find metadata for service id " + serviceId)
                    .withRequest(RpcRequestRecycler.newInstance(serviceId, requestId, methodId));
        }

        ByteBuf byteBuf = ctx.alloc().ioBuffer();
        byteBuf.writeInt(serviceId);
        byteBuf.writeLong(requestId);
        byteBuf.writeByte(methodId);
        byteBuf.writeByte(typeId);

        byteBuf.writeShort(EMPTY_VALUE);
        try (ByteBufOutputStream out = ByteBufOutputStreamRecycler.newInstance(byteBuf)) {
            RPC_METADATA_SERIALIZER.serialize(rpcServiceUnit.getRpcService(), out);
        } catch (Exception e) {
            ReferenceCountUtil.release(byteBuf);
            log.error("Can not serialize metadata to client", e);
            throw new RpcException(e).withError(e.getMessage())
                    .withStatus(SERIALIZER_ERROR)
                    .withRequest(RpcRequestRecycler.newInstance(serviceId, requestId, methodId));
        }
        return adjustLength(byteBuf);
    }

    private ByteBuf adjustLength(ByteBuf byteBuf) {
        byteBuf.markReaderIndex();
        byteBuf.readerIndex(16);
        int bodyLength = byteBuf.readableBytes();
        byteBuf.resetReaderIndex();

        byteBuf.markWriterIndex();
        byteBuf.writerIndex(14);
        byteBuf.writeShort(bodyLength);
        byteBuf.resetWriterIndex();
        return byteBuf;
    }

    private Object unmarshall(ByteBuf byteBuf, short bodyLength, RpcRequest request) {
        try (ByteBufInputStream in = ByteBufInputStreamRecycler.newInstance(byteBuf)) {
            return request.getMethodUnit().deserializeToParameter(in);
        } catch (Exception e) {
            log.warn("Can not convent to parameter for current com.lee.rpc request", e);
            throw new RpcException(e).withError(e.getMessage())
                    .withStatus(SERIALIZER_ERROR)
                    .withRequest(request);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        //所有decoder的错误，包装的都是DecoderException,所以在decoder阶段出现的错误我们需要获取内部的具体错误信息
        Throwable rootCause = cause.getCause();

        if (rootCause instanceof RpcException) {
            RpcException rpcException = (RpcException) rootCause;
            if (rpcException.getRpcRequest() != null) {
                ctx.channel().writeAndFlush(
                        rpcException.getRpcRequest().type(ABNORMAL).body(rpcException)
                );
            } else {
                log.error("Exception happened", cause);
            }
            return;
        }

        super.exceptionCaught(ctx, cause);
    }
}
