package com.lee.rpc.decoder;

import com.lee.rpc.helper.client.ClientProxy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

import static com.lee.rpc.util.Constant.EMPTY_VALUE;
import static com.lee.rpc.util.Constant.HEARTBEAT;

/**
 * 客户端只需要在指定时间段内没有发送消息的时候，再来发送心跳
 * 为了防止，只写不读的情况，所以读和写都需要更新sendHeartbeatNumber心跳次数
 *
 * @author l46li
 */
@Slf4j
public class ClientTimeOutHandler extends ChannelDuplexHandler {

    private final ClientProxy clientProxy;
    private final ByteBuf heartbeat = Unpooled.buffer(16);

    private int sendHeartbeatNumber;

    public ClientTimeOutHandler(ClientProxy clientProxy) {
        this.clientProxy = clientProxy;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        sendHeartbeatNumber = 0;
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        sendHeartbeatNumber = 0;
        super.write(ctx, msg, promise);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            sendHeartbeat(ctx);
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    private void sendHeartbeat(ChannelHandlerContext ctx) {
        heartbeat.clear()
                .writeInt(clientProxy.getServiceId())
                .writeLong(HEARTBEAT)
                .writeByte(HEARTBEAT)
                .writeByte(HEARTBEAT)
                .writeShort(EMPTY_VALUE);
        ctx.writeAndFlush(heartbeat).addListener(
                future -> {
                    if (!future.isSuccess()) {
                        if (sendHeartbeatNumber >= 3) {
                            //如果3次心跳都没有得到回复，那么直接失败, 由于当前Channel的close会导致
                            //inActive的方法调用，所以在inActive的逻辑里面就会重新发起到服务器的连接请求
                            ctx.channel().close();
                        } else {
                            sendHeartbeatNumber = sendHeartbeatNumber + 1;
                        }
                    } else {
                        log.info("Send heartbeat ping message to server {}", clientProxy.getServiceId());
                    }
                }
        );
    }
}
