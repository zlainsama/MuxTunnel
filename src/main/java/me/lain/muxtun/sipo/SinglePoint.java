package me.lain.muxtun.sipo;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import me.lain.muxtun.Shared;
import me.lain.muxtun.codec.FrameCodec;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.codec.MessageCodec;
import me.lain.muxtun.message.SnappyCodec;
import me.lain.muxtun.util.SimpleLogger;

public class SinglePoint
{

    private static final IntUnaryOperator decrementIfPositive = i -> i > 0 ? i - 1 : i;

    private final SinglePointConfig config;
    private final ChannelGroup channels;
    private final LinkManager manager;
    private final LinkHandler[] linkHandlers;
    private final TCPStreamHandler tcpStreamHandler;
    private final UDPStreamHandler udpStreamHandler;
    private final AtomicReference<Future<?>> scheduledMaintainLinks;

    public SinglePoint(SinglePointConfig config)
    {
        this.config = config;
        this.channels = new DefaultChannelGroup("SinglePoint", GlobalEventExecutor.INSTANCE, true);
        this.manager = new LinkManager(new SharedResources(future -> {
            if (future.isSuccess())
                channels.add(future.channel());
        }, config.getTargetAddress(), config.getMaxCLF()));
        this.linkHandlers = IntStream.range(0, config.getNumSessions()).mapToObj(i -> new LinkHandler()).toArray(LinkHandler[]::new);
        this.tcpStreamHandler = TCPStreamHandler.DEFAULT;
        this.udpStreamHandler = new UDPStreamHandler(manager);
        this.scheduledMaintainLinks = new AtomicReference<>();
    }

    private ChannelFuture initiateNewLink(LinkHandler linkHandler)
    {
        return new Bootstrap()
                .group(Vars.LINKS)
                .channel(Shared.NettyObjects.classSocketChannel)
                .handler(new ChannelInitializer<SocketChannel>()
                {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception
                    {
                        ch.attr(Vars.LINKCONTEXT_KEY).set(new LinkContext(manager, ch));

                        ch.pipeline().addLast(new IdleStateHandler(0, 0, 60)
                        {

                            @Override
                            protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception
                            {
                                if (evt.state() == IdleState.ALL_IDLE)
                                    ctx.channel().writeAndFlush(MessageType.PING.create());
                            }

                        });
                        ch.pipeline().addLast(new WriteTimeoutHandler(30));
                        ch.pipeline().addLast(config.getProxySupplier().get());
                        ch.pipeline().addLast(config.getSslCtx().newHandler(ch.alloc()));
                        ch.pipeline().addLast(new ChunkedWriteHandler());
                        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                        ch.pipeline().addLast(new SnappyCodec());
                        ch.pipeline().addLast(new FrameCodec());
                        ch.pipeline().addLast(MessageCodec.DEFAULT);
                        ch.pipeline().addLast(linkHandler);
                    }

                })
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .connect(config.getRemoteAddress())
                .addListener(manager.getResources().getChannelAccumulator())
                .addListener(new ChannelFutureListener()
                {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception
                    {
                        if (future.isSuccess())
                        {
                            RandomSession s = linkHandler.getRandomSession(false);
                            future.channel().writeAndFlush(MessageType.JOINSESSION.create().setId(s.getSessionId()).setBuf(Unpooled.wrappedBuffer(s.getChallenge())));
                        }
                    }

                });
    }

    public Future<Void> start()
    {
        Promise<Void> result = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
        GlobalEventExecutor.INSTANCE.submit(() -> {
            PromiseCombiner combiner = new PromiseCombiner(GlobalEventExecutor.INSTANCE);
            combiner.addAll(startTCPStreamService(), startUDPStreamService());
            combiner.finish(result);
        });
        return result.addListener(future -> {
            if (future.isSuccess())
                Optional.ofNullable(scheduledMaintainLinks.getAndSet(GlobalEventExecutor.INSTANCE.scheduleWithFixedDelay(() -> {
                    for (LinkHandler linkHandler : linkHandlers)
                    {
                        linkHandler.getLinksCount().updateAndGet(i -> {
                            if (i < config.getNumLinksPerSession())
                            {
                                i += 1;

                                initiateNewLink(linkHandler).addListener(new ChannelFutureListener()
                                {

                                    @Override
                                    public void operationComplete(ChannelFuture future) throws Exception
                                    {
                                        if (future.isSuccess())
                                        {
                                            future.channel().closeFuture().addListener(closeFuture -> {
                                                linkHandler.getLinksCount().updateAndGet(decrementIfPositive);

                                                future.channel().eventLoop().execute(() -> {
                                                    Throwable error = Vars.ChannelError.get(future.channel());
                                                    if (error != null)
                                                        SimpleLogger.println("%s > [%s] link %s closed with unexpected error. (%s)", Shared.printNow(), config.getName(), future.channel().id(), error);
                                                });
                                            });
                                        }
                                        else
                                        {
                                            linkHandler.getLinksCount().updateAndGet(decrementIfPositive);
                                        }
                                    }

                                });
                            }
                            return i;
                        });
                    }
                }, 1L, 1L, TimeUnit.SECONDS))).ifPresent(scheduled -> scheduled.cancel(false));
            else
                stop();
        });
    }

    private ChannelFuture startTCPStreamService()
    {
        return new ServerBootstrap()
                .group(Vars.BOSS, Vars.STREAMS)
                .channel(Shared.NettyObjects.classServerSocketChannel)
                .childHandler(new ChannelInitializer<SocketChannel>()
                {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception
                    {
                        ch.newSucceededFuture().addListener(manager.getResources().getChannelAccumulator());

                        RelayRequest request = manager.newTCPRelayRequest(ch.eventLoop());
                        if (!request.addListener(future -> {
                            if (future.isSuccess())
                            {
                                RelayRequestResult result = (RelayRequestResult) future.get();
                                StreamContext context = result.getSession().getStreams().compute(result.getStreamId(), (key, value) -> {
                                    if (value != null)
                                        throw new Error("BadServer");
                                    return new StreamContext(key, result.getSession(), ch);
                                });
                                ch.attr(Vars.STREAMCONTEXT_KEY).set(context);
                                ch.closeFuture().addListener(closeFuture -> {
                                    if (context.getSession().getStreams().remove(context.getStreamId(), context))
                                        context.getSession().writeAndFlush(MessageType.CLOSESTREAM.create().setId(context.getStreamId()));
                                });
                                ch.config().setAutoRead(true);
                            }
                        }).isDone())
                        {
                            ChannelFutureListener taskCancelRequest = future -> request.cancel(false);
                            ch.closeFuture().addListener(taskCancelRequest);
                            request.addListener(future -> ch.closeFuture().removeListener(taskCancelRequest));
                        }

                        ch.pipeline().addLast(new ChunkedWriteHandler());
                        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                        ch.pipeline().addLast(tcpStreamHandler);
                    }

                })
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.AUTO_READ, false)
                .bind(config.getBindAddress())
                .addListener(manager.getResources().getChannelAccumulator());
    }

    private ChannelFuture startUDPStreamService()
    {
        return new Bootstrap()
                .group(Vars.BOSS)
                .channel(Shared.NettyObjects.classDatagramChannel)
                .handler(new ChannelInitializer<DatagramChannel>()
                {

                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception
                    {
                        ch.pipeline().addLast(new ChunkedWriteHandler());
                        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                        ch.pipeline().addLast(udpStreamHandler);
                    }

                })
                .option(ChannelOption.AUTO_CLOSE, false)
                .bind(config.getBindAddress())
                .addListener(manager.getResources().getChannelAccumulator());
    }

    public Future<Void> stop()
    {
        return channels.close().addListener(future -> Optional.ofNullable(scheduledMaintainLinks.getAndSet(null)).ifPresent(scheduled -> scheduled.cancel(false)));
    }

    @Override
    public String toString()
    {
        return config.getName();
    }

}
