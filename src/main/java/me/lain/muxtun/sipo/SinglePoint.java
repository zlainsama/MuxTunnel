package me.lain.muxtun.sipo;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import me.lain.muxtun.Shared;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.codec.MessageCodec;
import me.lain.muxtun.sipo.config.LinkPath;
import me.lain.muxtun.sipo.config.SinglePointConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class SinglePoint {

    private static final Consumer<Future<?>> CANCEL_FUTURE = future -> future.cancel(false);
    private static final Optional<Channel> EMPTY_CHANNEL = Optional.empty();

    private final SinglePointConfig config;
    private final SslContext sslCtx;
    private final ChannelGroup channels;
    private final LinkManager manager;
    private final LinkHandler[] linkHandlers;
    private final TcpStreamHandler tcpStreamHandler;
    private final UdpStreamHandler udpStreamHandler;
    private final AtomicReference<Future<?>> scheduledMaintainTask;

    public SinglePoint(SinglePointConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        this.sslCtx = SinglePointConfig.buildContext(config.getPathCert(), config.getPathKey(), config.getTrusts(), config.getCiphers(), config.getProtocols());
        this.channels = new DefaultChannelGroup("SinglePoint", GlobalEventExecutor.INSTANCE, true);
        this.manager = new LinkManager(new SharedResources(this::addChannel, config.getTargetAddress(), config.getMaxCLF()));
        this.linkHandlers = IntStream.range(0, config.getNumSessions()).mapToObj(i -> new LinkHandler()).toArray(LinkHandler[]::new);
        this.tcpStreamHandler = TcpStreamHandler.DEFAULT;
        this.udpStreamHandler = new UdpStreamHandler(manager);
        this.scheduledMaintainTask = new AtomicReference<>();
    }

    private void addChannel(ChannelFuture future) {
        if (future.isSuccess())
            getChannels().add(future.channel());
    }

    private ChannelFuture initiateNewLink(LinkHandler linkHandler, LinkPath linkPath, int index) {
        return new Bootstrap()
                .group(Vars.WORKERS)
                .channel(Shared.NettyObjects.classSocketChannel)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.attr(Vars.LINKCONTEXT_KEY).set(new LinkContext(manager, ch, linkPath));

                        ch.pipeline().addLast(new IdleStateHandler(0, 0, 60) {

                            @Override
                            protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
                                if (evt.state() == IdleState.ALL_IDLE)
                                    ctx.channel().writeAndFlush(MessageType.PING.create());
                            }

                        });
                        ch.pipeline().addLast(new WriteTimeoutHandler(30));
                        ch.pipeline().addLast(linkPath.getProxySupplier().get());
                        ch.pipeline().addLast(Vars.HANDLERNAME_TLS, sslCtx.newHandler(ch.alloc(), Vars.SHARED_POOL));
                        ch.pipeline().addLast(Vars.HANDLERNAME_CODEC, new MessageCodec());
                        ch.pipeline().addLast(Vars.HANDLERNAME_HANDLER, linkHandler);
                    }

                })
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .connect(config.getRemoteAddress())
                .addListener(manager.getResources().getChannelAccumulator())
                .addListener((ChannelFutureListener) future -> {
                    Channel channel = future.channel();
                    if (future.isSuccess()) {
                        Optional<Channel> storedValue = Optional.of(channel);
                        if (linkHandler.getChannelMap().compute(index, (k, v) -> {
                            if (v == null || !v.isPresent())
                                return storedValue;
                            return v;
                        }).get() == channel) {
                            channel.closeFuture().addListener(closeFuture -> GlobalEventExecutor.INSTANCE.schedule(() -> linkHandler.getChannelMap().remove(index, storedValue), 2L, TimeUnit.SECONDS));
                            RandomSession s = linkHandler.getRandomSession(false);
                            channel.writeAndFlush(MessageType.JOINSESSION.create()
                                    .setId(s.getSessionId())
                                    .setId2(manager.getResources().getTargetAddress())
                                    .setBuf(Unpooled.wrappedBuffer(s.getChallenge())));
                        } else {
                            channel.close();
                        }
                    } else {
                        GlobalEventExecutor.INSTANCE.schedule(() -> linkHandler.getChannelMap().remove(index, EMPTY_CHANNEL), 5L, TimeUnit.SECONDS);
                    }
                });
    }

    public ChannelGroup getChannels() {
        return channels;
    }

    public Future<Void> start() {
        return Shared.NettyUtils.combineFutures(Arrays.asList(startTcpStreamService(), startUdpStreamService())).addListener(future -> {
            if (future.isSuccess())
                Optional.ofNullable(scheduledMaintainTask.getAndSet(GlobalEventExecutor.INSTANCE.scheduleWithFixedDelay(() -> {
                    manager.getSessions().values().forEach(LinkSession::tick);
                    for (LinkHandler linkHandler : linkHandlers) {
                        List<LinkPath> linkPaths = config.getLinkPaths();
                        int first = -1;
                        for (int i = 0; i < linkPaths.size(); i++) {
                            boolean[] computed = new boolean[]{false};
                            if (!linkHandler.getChannelMap().computeIfAbsent(i, k -> {
                                computed[0] = true;
                                return EMPTY_CHANNEL;
                            }).isPresent() && computed[0]) {
                                initiateNewLink(linkHandler, linkPaths.get(i), i);
                                first = i;
                                break;
                            }
                        }
                        for (int i = linkPaths.size() - 1; i >= 0 && i > first; i--) {
                            boolean[] computed = new boolean[]{false};
                            if (!linkHandler.getChannelMap().computeIfAbsent(i, k -> {
                                computed[0] = true;
                                return EMPTY_CHANNEL;
                            }).isPresent() && computed[0]) {
                                initiateNewLink(linkHandler, linkPaths.get(i), i);
                                break;
                            }
                        }
                    }
                }, 1L, 1L, TimeUnit.SECONDS))).ifPresent(CANCEL_FUTURE);
            else
                stop();
        });
    }

    private ChannelFuture startTcpStreamService() {
        return new ServerBootstrap()
                .group(Vars.WORKERS)
                .channel(Shared.NettyObjects.classServerSocketChannel)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.newSucceededFuture().addListener(manager.getResources().getChannelAccumulator());

                        RelayRequest request = manager.newTcpRelayRequest(ch.eventLoop());
                        if (!request.addListener(future -> {
                            if (future.isSuccess()) {
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
                        }).isDone()) {
                            ChannelFutureListener taskCancelRequest = future -> request.cancel(false);
                            ch.closeFuture().addListener(taskCancelRequest);
                            request.addListener(future -> ch.closeFuture().removeListener(taskCancelRequest));
                        }

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

    private ChannelFuture startUdpStreamService() {
        return new Bootstrap()
                .group(Vars.WORKERS)
                .channel(Shared.NettyObjects.classDatagramChannel)
                .handler(new ChannelInitializer<DatagramChannel>() {

                    @Override
                    protected void initChannel(DatagramChannel ch) throws Exception {
                        ch.pipeline().addLast(udpStreamHandler);
                    }

                })
                .option(ChannelOption.AUTO_CLOSE, false)
                .bind(config.getBindAddress())
                .addListener(manager.getResources().getChannelAccumulator());
    }

    public Future<Void> stop() {
        return getChannels().close().addListener(future -> {
            manager.getSessions().values().forEach(LinkSession::close);
            Optional.ofNullable(scheduledMaintainTask.getAndSet(null)).ifPresent(CANCEL_FUTURE);
        });
    }

}
