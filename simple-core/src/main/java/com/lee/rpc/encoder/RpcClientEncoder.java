package com.lee.rpc.encoder;

import com.lee.rpc.RpcException;
import com.lee.rpc.RpcRequest;
import com.lee.rpc.helper.client.ClientHelper;
import com.lee.rpc.helper.recycler.ByteBufOutputStreamRecycler;
import com.lee.rpc.helper.recycler.RpcRequestRecycler;
import com.lee.rpc.util.stream.ByteBufOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import static com.lee.rpc.util.Constant.*;
import static com.lee.rpc.util.exception.ErrorType.CLIENT_SERIALIZER_ERROR;

/**
 * 4          8           1         1     2              xxx bytes
 * serviceId  requestId   methodId  type  bodyLength     bodyContent
 *
 * @author Administrator
 */
@Slf4j
public class RpcClientEncoder extends MessageToByteEncoder<RpcRequest> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcRequest request, ByteBuf out) {
        try {
            int serviceId = request.getServiceId();
            byte methodId = request.getMethodId();
            byte typeId = request.getTypeId();

            out.writeInt(serviceId);
            out.writeLong(request.getRequestId());
            out.writeByte(methodId);
            out.writeByte(typeId);
            out.writeShort(EMPTY_VALUE);

            switch (typeId) {
                case OBJECT:
                    processRpcRequest(request, out);

                    out.markReaderIndex();
                    out.readerIndex(16);
                    int bodyLength = out.readableBytes();
                    out.resetReaderIndex();

                    out.markWriterIndex();
                    out.writerIndex(14);
                    out.writeShort(bodyLength);
                    out.resetWriterIndex();
                    break;
                case EMPTY_TYPE:
                    break;
                default:
                    throw new RpcException("Can not support type " + typeId);
            }
        } finally {
            //待验证，是否支持跨线程的回收, 已验证支持跨线程回收，可以在不同线程来进行释放操作
            RpcRequestRecycler.recycle(request);
        }
    }

    private void processRpcRequest(RpcRequest request, ByteBuf buffer) {
        try (ByteBufOutputStream out = ByteBufOutputStreamRecycler.newInstance(buffer)) {
            request.getMethodUnit().serializeParameter(request.getBody(), out);
        } catch (Exception e) {
            //注意：Encoder里面是不会抛出异常的，只是会单纯的给future一个错误的listener回调，所以这个地方需要捕获
            //异常，然后打印一下，因为这个地方是发送消息的逻辑，所以失败了也无所谓,客户端发送消息失败了,提示一下错误消息就可以了
            ReferenceCountUtil.release(buffer);
            log.error("Can not serialize parameter to ByteBuf", e);
            ClientHelper.setValue(
                    request.getRequestId(),
                    new RpcException(e).withStatus(CLIENT_SERIALIZER_ERROR).withError(e.getMessage())
            );
        }
    }
}
