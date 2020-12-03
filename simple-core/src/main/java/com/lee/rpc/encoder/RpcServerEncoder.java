package com.lee.rpc.encoder;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcExceptionSerializer;
import com.lee.rpc.RpcRequest;
import com.lee.rpc.helper.recycler.ByteBufOutputStreamRecycler;
import com.lee.rpc.helper.recycler.RpcRequestRecycler;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

import static com.lee.rpc.util.Constant.ABNORMAL;
import static com.lee.rpc.util.Constant.EMPTY_TYPE;
import static com.lee.rpc.util.Constant.EMPTY_VALUE;
import static com.lee.rpc.util.Constant.NORMAL;
import static com.lee.rpc.util.exception.ErrorType.SERIALIZER_ERROR;

/**
 * 4            8           1          1     2             xxx bytes
 * serviceId    requestId   methodId   type  bodyLength    bodyContext
 *
 * @author l46li
 */
@Slf4j
public class RpcServerEncoder extends MessageToByteEncoder<RpcRequest> {

    private static final RpcExceptionSerializer exceptionSerializer = new RpcExceptionSerializer();

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcRequest request, ByteBuf out) {
        try {
            int serviceId = request.getServiceId();
            byte typeId = request.getTypeId();

            out.writeInt(serviceId);
            out.writeLong(request.getRequestId());
            out.writeByte(request.getMethodId());
            out.writeByte(typeId);
            out.writeShort(EMPTY_VALUE);

            switch (typeId) {
                case NORMAL:
                    processReturnValue(request, out);
                    adjust(out);
                    break;
                case ABNORMAL:
                    processRpcException(request, out);
                    adjust(out);
                    break;
                case EMPTY_TYPE:
                    break;
                default:
                    throw new RpcException("Can not support type " + typeId);
            }
        } finally {
            RpcRequestRecycler.recycle(request);
        }
    }

    private void adjust(ByteBuf out) {
        out.markReaderIndex();
        out.readerIndex(16);
        int bodyLength = out.readableBytes();
        out.resetReaderIndex();

        out.markWriterIndex();
        out.writerIndex(14);
        out.writeShort(bodyLength);
        out.resetWriterIndex();
    }

    private void processReturnValue(RpcRequest request, ByteBuf byteBuf) {
        try (ByteBufOutputStream out = ByteBufOutputStreamRecycler.newInstance(byteBuf)) {
            request.getMethodUnit().serializeReturnValue(request.getBody(), out);
        } catch (Exception e) {
            log.error("Can not deserialize " + request.getBody(), e);

            //因为出现了错误，所需需要修改typeId类型为不正常ABNORMAL，然后在发送消息
            byteBuf.markWriterIndex();
            byteBuf.writerIndex(13);
            byteBuf.writeShort(ABNORMAL);
            byteBuf.resetWriterIndex();

            processRpcException(
                    request.body(
                            new RpcException(e).withStatus(SERIALIZER_ERROR).withError(e.getMessage()
                            )
                    ), byteBuf);
        }
    }

    private void processRpcException(RpcRequest request, ByteBuf byteBuf) {
        try (ByteBufOutputStream out = ByteBufOutputStreamRecycler.newInstance(byteBuf)) {
            exceptionSerializer.serialize((RpcException) request.getBody(), out);
        } catch (Exception e) {
            //基本上不会出现的，因为错误处理框架部分实现的，所以一般都是正常的,而且写数据失败，是不会想decoder那样发送
            //fireExceptionCaught的，所以这个地方就只是单纯的打印一下日志就可以了
            log.error("Can not serialize RpcException", e);
        }
    }
}
