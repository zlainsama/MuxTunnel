package me.lain.muxtun.sipo;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

@Sharable
class UDPStreamInitializer extends ChannelInitializer<DatagramChannel>
{

    private final UDPStreamInboundHandler inboundHandler;

    UDPStreamInitializer(UDPStreamInboundHandler inboundHandler)
    {
        this.inboundHandler = inboundHandler;
    }

    @Override
    protected void initChannel(DatagramChannel ch) throws Exception
    {
        ch.pipeline().addLast(new ChunkedWriteHandler());
        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
        ch.pipeline().addLast(inboundHandler);
        ch.pipeline().addLast(UDPStreamExceptionHandler.DEFAULT);
    }

}
