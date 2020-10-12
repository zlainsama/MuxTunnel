package me.lain.muxtun.sipo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.Shared;
import me.lain.muxtun.util.SimpleLogger;

@Sharable
class TcpStreamHandler extends ChannelInboundHandlerAdapter {

    static final TcpStreamHandler DEFAULT = new TcpStreamHandler();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf cast = (ByteBuf) msg;

            try {
                handleMessage(StreamContext.getContext(ctx.channel()), cast);
            } finally {
                ReferenceCountUtil.release(cast);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close().addListener(future -> SimpleLogger.println("%s > tcp stream connection %s closed with unexpected error. (%s)", Shared.printNow(), ctx.channel().id(), cause));
    }

    private void handleMessage(StreamContext sctx, ByteBuf msg) throws Exception {
        if (sctx.isActive() && !sctx.getPayloadWriter().writeSlices(msg.retain()))
            sctx.close();
    }

}
