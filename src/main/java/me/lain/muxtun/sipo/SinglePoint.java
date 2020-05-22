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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import me.lain.muxtun.Shared;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.codec.MessageCodec;
import me.lain.muxtun.util.SimpleLogger;

public class SinglePoint
{

    private static final IntUnaryOperator decrementIfPositive = i -> i > 0 ? i - 1 : i;

    private final SinglePointConfig config;
    private final ChannelGroup channels;
    private final LinkManager manager;
    private final LinkHandler[] linkHandlers;
    private final TcpStreamHandler tcpStreamHandler;
    private final UdpStreamHandler udpStreamHandler;
    private final AtomicReference<Future<?>> scheduledMaintainTask;

    public SinglePoint(SinglePointConfig config)
    {
        this.config = config;
        this.channels = new DefaultChannelGroup("SinglePoint", GlobalEventExecutor.INSTANCE, true);
        this.manager = new LinkManager(new SharedResources(future -> {
            if (future.isSuccess())
                channels.add(future.channel());
        }, config.getTargetAddress(), config.getMaxCLF()));
        this.linkHandlers = IntStream.range(0, config.getNumSessions()).mapToObj(i -> new LinkHandler()).toArray(LinkHandler[]::new);
        this.tcpStreamHandler = TcpStreamHandler.DEFAULT;
        this.udpStreamHandler = new UdpStreamHandler(manager);
        this.scheduledMaintainTask = new AtomicReference<>();
    }

    private ChannelFuture initiateNewLink(LinkHandler linkHandler)
    {
        return new Bootstrap()
                .group(Vars.WORKERS)
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
                        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                        ch.pipeline().addLast(config.getProxySupplier().get());
                        ch.pipeline().addLast(config.getSslCtx().newHandler(ch.alloc()));
                        ch.pipeline().addLast(new MessageCodec());
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
                            future.channel().writeAndFlush(MessageType.JOINSESSION.create()
                                    .setId(s.getSessionId())
                                    .setId2(manager.getResources().getTargetAddress())
                                    .setBuf(Unpooled.wrappedBuffer(s.getChallenge())));
                        }
                    }

                });
    }

    public Future<Void> start()
    {
        Promise<Void> result = GlobalEventExecutor.INSTANCE.newPromise();
        GlobalEventExecutor.INSTANCE.execute(() -> {
            PromiseCombiner combiner = new PromiseCombiner(GlobalEventExecutor.INSTANCE);
            combiner.addAll(startTcpStreamService(), startUdpStreamService());
            combiner.finish(result);
        });
        return result.addListener(future -> {
            if (future.isSuccess())
                Optional.ofNullable(scheduledMaintainTask.getAndSet(GlobalEventExecutor.INSTANCE.scheduleWithFixedDelay(() -> {
                    manager.getSessions().values().forEach(session -> {
                        if (!session.getMembers().isEmpty())
                            session.getTimeoutCounter().set(0);
                        else if (session.getTimeoutCounter().incrementAndGet() > 30)
                            session.close();
                    });
                    for (LinkHandler linkHandler : linkHandlers)
                    {
                        int[] old = new int[1];
                        if (linkHandler.getLinksCount().updateAndGet(i -> {
                            old[0] = i;
                            if (i < config.getNumLinksPerSession())
                                i += 1;
                            return i;
                        }) != old[0])
                        {
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
                                                {
                                                    SimpleLogger.printStackTrace(error);
                                                    SimpleLogger.println("%s > [%s] link %s closed with unexpected error. (%s)", Shared.printNow(), config.getName(), future.channel().id(), error);
                                                }
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
                    }
                }, 1L, 1L, TimeUnit.SECONDS))).ifPresent(scheduled -> scheduled.cancel(false));
            else
                stop();
        });
    }

    private ChannelFuture startTcpStreamService()
    {
        return new ServerBootstrap()
                .group(Vars.WORKERS)
                .channel(Shared.NettyObjects.classServerSocketChannel)
                .childHandler(new ChannelInitializer<SocketChannel>()
                {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception
                    {
                        ch.newSucceededFuture().addListener(manager.getResources().getChannelAccumulator());

                        RelayRequest request = manager.newTcpRelayRequest(ch.eventLoop());
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

    private ChannelFuture startUdpStreamService()
    {
        return new Bootstrap()
                .group(Vars.WORKERS)
                .channel(Shared.NettyObjects.classDatagramChannel)
                .handler(new ChannelInitializer<DatagramChannel>()
                {

                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception
                    {
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
        return channels.close().addListener(future -> {
            manager.getSessions().values().forEach(LinkSession::close);
            Optional.ofNullable(scheduledMaintainTask.getAndSet(null)).ifPresent(scheduled -> scheduled.cancel(false));
        });
    }

    @Override
    public String toString()
    {
        return config.getName();
    }

}
