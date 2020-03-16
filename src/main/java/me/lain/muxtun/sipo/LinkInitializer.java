package me.lain.muxtun.sipo;

import java.util.Optional;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import me.lain.muxtun.codec.FrameCodec;
import me.lain.muxtun.codec.MessageCodec;

@Sharable
class LinkInitializer extends ChannelInitializer<SocketChannel>
{

    private static final ChannelFutureListener CLOSESTREAMS = future -> {
        future.channel().attr(Vars.SESSION_KEY).get().ongoingStreams.values().forEach(Channel::close);
    };

    private final SinglePointConfig config;
    private final LinkEventHandler eventHandler;

    LinkInitializer(SinglePointConfig config, LinkEventHandler eventHandler)
    {
        this.config = config;
        this.eventHandler = eventHandler;
    }

    private Optional<ChannelTrafficShapingHandler> getChannelTrafficShapingHandler()
    {
        if (config.writeLimit == 0L && config.readLimit == 0L)
            return Optional.empty();
        else
            return Optional.of(new ChannelTrafficShapingHandler(config.writeLimit, config.readLimit));
    }

    private Optional<GlobalTrafficShapingHandler> getGlobalTrafficShapingHandler()
    {
        return SinglePointConfig.getGlobalTrafficShapingHandler();
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception
    {
        ch.attr(Vars.SESSION_KEY).set(new LinkSession(config));
        ch.closeFuture().addListener(CLOSESTREAMS);

        ch.pipeline().addLast(new IdleStateHandler(0, 0, 60));
        ch.pipeline().addLast(new WriteTimeoutHandler(30));
        getGlobalTrafficShapingHandler().ifPresent(ch.pipeline()::addLast);
        getChannelTrafficShapingHandler().ifPresent(ch.pipeline()::addLast);
        ch.pipeline().addLast(new ChunkedWriteHandler());
        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
        ch.pipeline().addLast(config.proxySupplier.get());
        ch.pipeline().addLast(config.sslCtx.newHandler(ch.alloc()));
        ch.pipeline().addLast("FrameCodec", new FrameCodec());
        ch.pipeline().addLast("MessageCodec", MessageCodec.DEFAULT);
        ch.pipeline().addLast(LinkWriteMonitor.DEFAULT);
        ch.pipeline().addLast(LinkInboundHandler.DEFAULT);
        ch.pipeline().addLast(LinkWritabilityChangeListener.DEFAULT);
        ch.pipeline().addLast(eventHandler);
        ch.pipeline().addLast(LinkExceptionHandler.DEFAULT);
    }

}
