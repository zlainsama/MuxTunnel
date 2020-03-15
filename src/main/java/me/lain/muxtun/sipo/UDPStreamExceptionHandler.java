package me.lain.muxtun.sipo;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@Sharable
class UDPStreamExceptionHandler extends ChannelInboundHandlerAdapter
{

    static final UDPStreamExceptionHandler DEFAULT = new UDPStreamExceptionHandler();

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
    }

}
