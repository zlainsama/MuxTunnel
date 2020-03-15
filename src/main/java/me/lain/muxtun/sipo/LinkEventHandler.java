package me.lain.muxtun.sipo;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;

@Sharable
class LinkEventHandler extends ChannelInboundHandlerAdapter
{

    private final LinkSessionEventHandler handler;

    LinkEventHandler(LinkSessionEventHandler handler)
    {
        this.handler = handler;
    }

    private void handleEvent(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception
    {
        if (evt.state() == IdleState.ALL_IDLE)
        {
            if (ctx.channel().attr(Vars.SESSION_KEY).get().authStatus.completed)
            {
                ctx.writeAndFlush(new Message()
                        .setType(MessageType.Ping));
            }
            else
            {
                ctx.close();
            }
        }
    }

    private void handleEvent(ChannelHandlerContext ctx, LinkSessionEvent evt) throws Exception
    {
        handler.handleEvent(ctx.channel(), ctx.channel().attr(Vars.SESSION_KEY).get(), evt);
    }

    private void handleEvent(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt) throws Exception
    {
        if (!evt.isSuccess())
            ctx.close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        if (evt instanceof LinkSessionEvent)
        {
            handleEvent(ctx, (LinkSessionEvent) evt);
        }
        else if (evt instanceof IdleStateEvent)
        {
            handleEvent(ctx, (IdleStateEvent) evt);
        }
        else if (evt instanceof SslHandshakeCompletionEvent)
        {
            handleEvent(ctx, (SslHandshakeCompletionEvent) evt);
        }
        else
        {
            ctx.fireUserEventTriggered(evt);
        }
    }

}
