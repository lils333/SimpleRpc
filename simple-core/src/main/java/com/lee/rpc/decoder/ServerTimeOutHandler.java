package com.lee.rpc.decoder;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * 定时逻辑是由客户端发起的，所以整个链路中不通的情况只有可能是：
 * 服务端接收，服务端发送，客户端接收。也就是说，只有客户端的 pong，服务端的 ping，pong 的检测是有意义的
 *
 * @author l46li
 */
@Slf4j
public class ServerTimeOutHandler extends ChannelDuplexHandler {

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            //服务端比较除暴简单，只要发现200秒没有写入数据或者读取数据，那么就直接关闭掉该Channel
            //心跳包由客服端发起就可以了，毕竟是客服端发起建立的连接，谁发起谁维护
            ctx.channel().close();
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            //IO异常一般情况下都是在客户端关闭了链接的情况下，这个时候，服务器这边只需要简单的打印一下日志就可以了，不需要在做其他
            //处理了
            log.debug("IOException happen, may be client close this connection", cause);
        } else {
            super.exceptionCaught(ctx, cause);
        }
    }
}
