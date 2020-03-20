package me.lain.muxtun.sipo;

import java.util.Comparator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntUnaryOperator;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.concurrent.ScheduledFuture;
import me.lain.muxtun.Shared;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.codec.SnappyCodec;
import me.lain.muxtun.util.SimpleLogger;

public class SinglePoint
{

    private static final Comparator<Channel> linkSorter = Comparator.comparingInt(link -> {
        LinkSession session = link.attr(Vars.SESSION_KEY).get();
        return session.ongoingStreams.size() * 16 + session.pendingOpen.get() * 4 + session.clf.getFactor().orElse(8);
    });
    private static final IntUnaryOperator decrementIfPositive = i -> i > 0 ? i - 1 : i;

    private final SinglePointConfig config;
    private final ChannelGroup channels;
    private final ChannelGroup links;
    private final AtomicInteger pendingLinks;
    private final LinkInitializer linkInitializer;
    private final TCPStreamInitializer tcpStreamInitializer;
    private final UDPStreamInitializer udpStreamInitializer;
    private final Queue<RelayRequest> tcpRelayRequestQueue;
    private final Queue<RelayRequest> udpRelayRequestQueue;
    private final Queue<PendingOpenDrop> tcpPendingOpenDropQueue;
    private final Queue<PendingOpenDrop> udpPendingOpenDropQueue;
    private final Runnable taskMaintainLinks;
    private final Runnable taskTryOpenTCP;
    private final Runnable taskTryOpenUDP;
    private final AtomicReference<Optional<ScheduledFuture<?>>> scheduledMaintainLinks;

    public SinglePoint(SinglePointConfig config)
    {
        this.config = config;
        this.channels = new DefaultChannelGroup("SinglePoint", GlobalEventExecutor.INSTANCE, true);
        this.links = new DefaultChannelGroup("SinglePointLinks", GlobalEventExecutor.INSTANCE, true);
        this.pendingLinks = new AtomicInteger();
        this.linkInitializer = new LinkInitializer(config, new LinkEventHandler(this::handleLinkSessionEvent));
        this.tcpStreamInitializer = new TCPStreamInitializer(channels, this::enqueueTCPStream);
        this.udpStreamInitializer = new UDPStreamInitializer(new UDPStreamInboundHandler(channels, this::enqueueUDPStream));
        this.tcpRelayRequestQueue = new ConcurrentLinkedQueue<>();
        this.udpRelayRequestQueue = new ConcurrentLinkedQueue<>();
        this.tcpPendingOpenDropQueue = new ConcurrentLinkedQueue<>();
        this.udpPendingOpenDropQueue = new ConcurrentLinkedQueue<>();
        this.taskMaintainLinks = newTaskMaintainLinks();
        this.taskTryOpenTCP = newTaskTryOpenTCP();
        this.taskTryOpenUDP = newTaskTryOpenUDP();
        this.scheduledMaintainLinks = new AtomicReference<>(Optional.empty());
    }

    private RelayRequest enqueueTCPStream(Channel tcpStream)
    {
        RelayRequest request = new RelayRequest(tcpStream.eventLoop());

        PendingOpenDrop pendingOpenDrop;
        while ((pendingOpenDrop = tcpPendingOpenDropQueue.poll()) != null)
        {
            if (!pendingOpenDrop.scheduledDrop.cancel(false))
                continue;
            if (request.trySuccess(pendingOpenDrop.result))
                return request;
            pendingOpenDrop.dropStream.run();
        }

        if (!tcpRelayRequestQueue.offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
        {
            taskTryOpenTCP.run();
            ScheduledFuture<?> taskRequestMore = tcpStream.eventLoop().schedule(taskTryOpenTCP, 2L, TimeUnit.SECONDS);
            ScheduledFuture<?> taskCancelRequest = tcpStream.eventLoop().schedule(() -> request.cancel(false), 60L, TimeUnit.SECONDS);
            request.addListener(future -> {
                taskRequestMore.cancel(false);
                taskCancelRequest.cancel(false);
            });
        }
        return request;
    }

    private RelayRequest enqueueUDPStream(Channel udpStream)
    {
        RelayRequest request = new RelayRequest(udpStream.eventLoop());

        PendingOpenDrop pendingOpenDrop;
        while ((pendingOpenDrop = udpPendingOpenDropQueue.poll()) != null)
        {
            if (!pendingOpenDrop.scheduledDrop.cancel(false))
                continue;
            if (request.trySuccess(pendingOpenDrop.result))
                return request;
            pendingOpenDrop.dropStream.run();
        }

        if (!udpRelayRequestQueue.offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
        {
            taskTryOpenUDP.run();
            ScheduledFuture<?> taskRequestMore = udpStream.eventLoop().schedule(taskTryOpenUDP, 2L, TimeUnit.SECONDS);
            ScheduledFuture<?> taskCancelRequest = udpStream.eventLoop().schedule(() -> request.cancel(false), 60L, TimeUnit.SECONDS);
            request.addListener(future -> {
                taskRequestMore.cancel(false);
                taskCancelRequest.cancel(false);
            });
        }
        return request;
    }

    public ChannelGroup getChannels()
    {
        return channels;
    }

    private void handleLinkSessionEvent(Channel linkChannel, LinkSession session, LinkSessionEvent event) throws Exception
    {
        switch (event.type)
        {
            case AUTH:
            {
                if (event.clfLimitExceeded)
                {
                    linkChannel.close();
                    pendingLinks.updateAndGet(decrementIfPositive);
                }
                else
                {
                    linkChannel.writeAndFlush(MessageType.SNAPPY.create());
                    linkChannel.pipeline().addBefore("FrameCodec", "SnappyCodec", new SnappyCodec());

                    links.add(linkChannel);
                    pendingLinks.updateAndGet(decrementIfPositive);

                    SimpleLogger.println("%s > [%s] link %s up.",
                            Shared.printNow(),
                            config.name,
                            String.format("%s%s", linkChannel.id(), session.clf.getFactor().map(f -> String.format("(CLF:%d)", f)).orElse("")));
                    linkChannel.closeFuture().addListener(future -> {
                        linkChannel.eventLoop().submit(() -> {
                            SimpleLogger.println("%s > [%s] link %s down.%s%s",
                                    Shared.printNow(),
                                    config.name,
                                    String.format("%s%s", linkChannel.id(), session.clf.getFactor().map(f -> String.format("(CLF:%d)", f)).orElse("")),
                                    session.ongoingStreams.size() > 0 ? String.format(" (%d dropped)", session.ongoingStreams.size()) : "",
                                    linkChannel.attr(Vars.ERROR_KEY).get() != null ? String.format(" (%s)", linkChannel.attr(Vars.ERROR_KEY).get().toString()) : "");
                        });
                    });

                    if (!tcpRelayRequestQueue.isEmpty())
                        linkChannel.eventLoop().submit(taskTryOpenTCP);
                    if (!udpRelayRequestQueue.isEmpty())
                        linkChannel.eventLoop().submit(taskTryOpenUDP);
                }
                break;
            }
            case OPEN:
            {
                if (event.clfLimitExceeded)
                {
                    if (session.ongoingStreams.isEmpty())
                        linkChannel.close();
                    else
                    {
                        linkChannel.writeAndFlush(MessageType.DROP.create().setStreamId(event.streamId));
                        session.pendingOpen.updateAndGet(decrementIfPositive);
                    }

                    if (!tcpRelayRequestQueue.isEmpty())
                        linkChannel.eventLoop().submit(taskTryOpenTCP);
                }
                else
                {
                    RelayRequest request;
                    while ((request = tcpRelayRequestQueue.poll()) != null)
                    {
                        if (!request.setUncancellable())
                            continue;
                        break;
                    }

                    RelayRequestResult result = new RelayRequestResult(linkChannel, session, event.streamId);
                    if (request == null || request.isDone() || !request.trySuccess(result))
                    {
                        Runnable dropStream = () -> {
                            linkChannel.writeAndFlush(MessageType.DROP.create().setStreamId(event.streamId));
                            session.pendingOpen.updateAndGet(decrementIfPositive);
                        };
                        ScheduledFuture<?> scheduledDrop = linkChannel.eventLoop().schedule(dropStream, 1L, TimeUnit.SECONDS);

                        if (!tcpPendingOpenDropQueue.offer(new PendingOpenDrop(result, dropStream, scheduledDrop)) && scheduledDrop.cancel(false))
                            dropStream.run();
                    }
                    else
                    {
                        session.pendingOpen.updateAndGet(decrementIfPositive);
                    }
                }
                break;
            }
            case OPENUDP:
            {
                if (event.clfLimitExceeded)
                {
                    if (session.ongoingStreams.isEmpty())
                        linkChannel.close();
                    else
                    {
                        linkChannel.writeAndFlush(MessageType.DROP.create().setStreamId(event.streamId));
                        session.pendingOpen.updateAndGet(decrementIfPositive);
                    }

                    if (!udpRelayRequestQueue.isEmpty())
                        linkChannel.eventLoop().submit(taskTryOpenUDP);
                }
                else
                {
                    RelayRequest request;
                    while ((request = udpRelayRequestQueue.poll()) != null)
                    {
                        if (!request.setUncancellable())
                            continue;
                        break;
                    }

                    RelayRequestResult result = new RelayRequestResult(linkChannel, session, event.streamId);
                    if (request == null || request.isDone() || !request.trySuccess(result))
                    {
                        Runnable dropStream = () -> {
                            linkChannel.writeAndFlush(MessageType.DROP.create().setStreamId(event.streamId));
                            session.pendingOpen.updateAndGet(decrementIfPositive);
                        };
                        ScheduledFuture<?> scheduledDrop = linkChannel.eventLoop().schedule(dropStream, 1L, TimeUnit.SECONDS);

                        if (!udpPendingOpenDropQueue.offer(new PendingOpenDrop(result, dropStream, scheduledDrop)) && scheduledDrop.cancel(false))
                            dropStream.run();
                    }
                    else
                    {
                        session.pendingOpen.updateAndGet(decrementIfPositive);
                    }
                }
                break;
            }
            case OPENFAILED:
            {
                if (event.clfLimitExceeded && session.ongoingStreams.isEmpty())
                {
                    linkChannel.close();
                }
                else
                {
                    session.pendingOpen.updateAndGet(decrementIfPositive);
                }
                break;
            }
            case PING:
            {
                if (event.clfLimitExceeded && session.ongoingStreams.isEmpty())
                {
                    linkChannel.close();
                }
                else
                {
                    if (!tcpRelayRequestQueue.isEmpty())
                        linkChannel.eventLoop().submit(taskTryOpenTCP);
                    if (!udpRelayRequestQueue.isEmpty())
                        linkChannel.eventLoop().submit(taskTryOpenUDP);
                }
                break;
            }
        }
    }

    private ChannelFuture initiateNewLink()
    {
        return new Bootstrap()
                .group(Shared.NettyObjects.workerGroup)
                .channel(Shared.NettyObjects.classSocketChannel)
                .handler(linkInitializer)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .connect(config.remoteAddress)
                .addListener(new ChannelFutureListener()
                {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception
                    {
                        if (future.isSuccess())
                        {
                            channels.add(future.channel());

                            if (config.secret_3 != null && Shared.isSHA3Available())
                                future.channel().writeAndFlush(MessageType.AUTHREQ3.create().setPayload(Unpooled.EMPTY_BUFFER));
                            else if (config.secret != null)
                                future.channel().writeAndFlush(MessageType.AUTHREQ.create().setPayload(Unpooled.EMPTY_BUFFER));
                            else
                                future.channel().close();

                            future.channel().closeFuture().addListener(new ChannelFutureListener()
                            {

                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception
                                {
                                    if (!future.channel().attr(Vars.SESSION_KEY).get().authStatus.completed)
                                        pendingLinks.updateAndGet(decrementIfPositive);
                                }

                            });
                        }
                    }

                });
    }

    private Runnable newTaskMaintainLinks()
    {
        return new Runnable()
        {

            private final ChannelFutureListener decrementPendingIfFailed = future -> {
                if (!future.isSuccess())
                    pendingLinks.updateAndGet(decrementIfPositive);
            };

            @Override
            public void run()
            {
                try
                {
                    if (shouldTry())
                        tryConnect();
                }
                catch (Throwable ignored)
                {
                }
            }

            private boolean shouldTry()
            {
                return (links.size() + pendingLinks.get()) < config.numLinks && !Shared.NettyObjects.workerGroup.isShuttingDown();
            }

            private void tryConnect()
            {
                if (tryIncrementPending())
                    initiateNewLink().addListener(decrementPendingIfFailed);
            }

            private boolean tryIncrementPending()
            {
                while (true)
                {
                    final int limit = config.numLinks - links.size();
                    final int pending = pendingLinks.get();

                    if (pending >= limit)
                        return false;

                    if (pendingLinks.compareAndSet(pending, pending + 1))
                        return true;
                }
            }

        };
    }

    private Runnable newTaskTryOpenTCP()
    {
        return new Runnable()
        {

            private final Message msg = MessageType.OPEN.create().setStreamId(config.targetAddress);
            private final ChannelFutureListener decrementPendingIfFailed = future -> {
                if (!future.isSuccess())
                    future.channel().attr(Vars.SESSION_KEY).get().pendingOpen.updateAndGet(decrementIfPositive);
            };

            @Override
            public void run()
            {
                if (!links.isEmpty() && !tcpRelayRequestQueue.isEmpty())
                {
                    links.stream().sorted(linkSorter).limit(config.limitOpen).forEach(link -> {
                        link.attr(Vars.SESSION_KEY).get().pendingOpen.incrementAndGet();
                        link.writeAndFlush(msg).addListener(decrementPendingIfFailed);
                    });
                }
            }

        };
    }

    private Runnable newTaskTryOpenUDP()
    {
        return new Runnable()
        {

            private final Message msg = MessageType.OPENUDP.create().setStreamId(config.targetAddress);
            private final ChannelFutureListener decrementPendingIfFailed = future -> {
                if (!future.isSuccess())
                    future.channel().attr(Vars.SESSION_KEY).get().pendingOpen.updateAndGet(decrementIfPositive);
            };

            @Override
            public void run()
            {
                if (!links.isEmpty() && !udpRelayRequestQueue.isEmpty())
                {
                    links.stream().sorted(linkSorter).limit(config.limitOpen).forEach(link -> {
                        link.attr(Vars.SESSION_KEY).get().pendingOpen.incrementAndGet();
                        link.writeAndFlush(msg).addListener(decrementPendingIfFailed);
                    });
                }
            }

        };
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
                scheduledMaintainLinks.getAndSet(Optional.of(GlobalEventExecutor.INSTANCE.scheduleWithFixedDelay(taskMaintainLinks, 1L, 1L, TimeUnit.SECONDS))).ifPresent(scheduled -> scheduled.cancel(false));
            else
                stop();
        });
    }

    private ChannelFuture startTCPStreamService()
    {
        return new ServerBootstrap()
                .group(Shared.NettyObjects.bossGroup, Shared.NettyObjects.workerGroup)
                .channel(Shared.NettyObjects.classServerSocketChannel)
                .childHandler(tcpStreamInitializer)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .bind(config.bindAddress)
                .addListener(new ChannelFutureListener()
                {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception
                    {
                        if (future.isSuccess())
                            channels.add(future.channel());
                    }

                });
    }

    private ChannelFuture startUDPStreamService()
    {
        return new Bootstrap()
                .group(Shared.NettyObjects.workerGroup)
                .channel(Shared.NettyObjects.classDatagramChannel)
                .handler(udpStreamInitializer)
                .option(ChannelOption.AUTO_CLOSE, false)
                .bind(config.bindAddress)
                .addListener(new ChannelFutureListener()
                {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception
                    {
                        if (future.isSuccess())
                            channels.add(future.channel());
                    }

                });
    }

    public Future<Void> stop()
    {
        return channels.close().addListener(future -> scheduledMaintainLinks.getAndSet(Optional.empty()).ifPresent(scheduled -> scheduled.cancel(false)));
    }

    @Override
    public String toString()
    {
        return config.name;
    }

}
