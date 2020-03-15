package me.lain.muxtun.sipo;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import me.lain.muxtun.codec.Message;

@Sharable
class LinkWriteMonitor extends ChannelOutboundHandlerAdapter
{

    private static final ChannelFutureListener INITIATECONNECTIONLATENCYFACTORCALCULATION = future -> {
        if (future.isSuccess())
            future.channel().attr(Vars.SESSION_KEY).get().clf.initiateCalculation();
    };

    static final LinkWriteMonitor DEFAULT = new LinkWriteMonitor();

    private void handleMessageWrite(ChannelHandlerContext ctx, Message msg, ChannelPromise promise) throws Exception
    {
        switch (msg.getType())
        {
            case Ping:
            case Open:
            case OpenUDP:
            case Auth:
                promise.addListener(INITIATECONNECTIONLATENCYFACTORCALCULATION);
                break;
            default:
                break;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        if (msg instanceof Message)
            handleMessageWrite(ctx, (Message) msg, promise);

        ctx.write(msg, promise);
    }

}
