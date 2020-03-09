package me.lain.muxtun;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GlobalEventExecutor;
import me.lain.muxtun.codec.FrameCodec;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.codec.MessageCodec;

public class SinglePoint
{

    public static final int DEFAULT_NUMLINKS = 4;
    public static final int DEFAULT_LIMITOPEN = 3;
    public static final int DEFAULT_MAXCLF = 8;

    private static final AttributeKey<Throwable> CHANNELERROR_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#ChannelError");
    private static final AttributeKey<Set<UUID>> ONGOINGSTREAMS_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#OngoingStreams");
    private static final AttributeKey<AtomicInteger> PENDINGOPEN_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#PendingOpen");
    private static final AttributeKey<Optional<Integer>> CONNECTIONLATENCYFACTOR_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#ConnectionLatencyFactor");
    private static final AttributeKey<Optional<Supplier<Integer>>> CONNECTIONLATENCYFACTORCALCULATOR_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#ConnectionLatencyFactorCalculator");
    private static final AttributeKey<Boolean> AUTHSTATUS_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#AuthStatus");
    private static final AttributeKey<Boolean> CHALLENGE_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#Challenge");
    private static final AttributeKey<UUID> STREAMID_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#StreamId");
    private static final AttributeKey<Map<InetSocketAddress, UUID>> STREAMIDUDP_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#StreamIdUDP");
    private static final AttributeKey<Map<InetSocketAddress, Queue<ByteBuf>>> PENDINGPAYLOADSUDP_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#PendingPayloadsUDP");

    private static final Comparator<Channel> linkSorter = Comparator.comparingInt(link -> {
        return link.attr(ONGOINGSTREAMS_KEY).get().size() * 4 + link.attr(PENDINGOPEN_KEY).get().get() * 2 + link.attr(CONNECTIONLATENCYFACTOR_KEY).get().orElse(8);
    });
    private static final IntUnaryOperator decrementIfPositive = i -> {
        return i > 0 ? i - 1 : i;
    };
    private static final Consumer<Channel> initiateConnectionLatencyFactorCalculator = link -> {
        if (!link.attr(CONNECTIONLATENCYFACTORCALCULATOR_KEY).get().isPresent())
        {
            long startTime = System.currentTimeMillis();
            link.attr(CONNECTIONLATENCYFACTORCALCULATOR_KEY).compareAndSet(Optional.empty(), Optional.of(() -> {
                long latency = Math.min(Math.max(0L, System.currentTimeMillis() - startTime), 1000L);
                return (int) (latency / 125L);
            }));
        }
    };
    private static final Function<Channel, Optional<Integer>> completeConnectionLatencyFactorCalculator = link -> {
        if (link.attr(CONNECTIONLATENCYFACTORCALCULATOR_KEY).get().isPresent())
        {
            link.attr(CONNECTIONLATENCYFACTORCALCULATOR_KEY).getAndSet(Optional.empty()).map(Supplier::get).ifPresent(factor -> {
                link.attr(CONNECTIONLATENCYFACTOR_KEY).set(Optional.of(factor));
            });
        }
        return link.attr(CONNECTIONLATENCYFACTOR_KEY).get();
    };

    private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final ChannelGroup links = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final AtomicInteger pendingLinks = new AtomicInteger();
    private final Map<UUID, Channel> inboundStreams = new ConcurrentHashMap<>();
    private final Map<UUID, InetSocketAddress> inboundStreamsUDP = new ConcurrentHashMap<>();
    private final Map<UUID, Channel> outboundStreams = new ConcurrentHashMap<>();
    private final Queue<Channel> pendingStreams = new ConcurrentLinkedQueue<>();
    private final Queue<InetSocketAddress> pendingStreamsUDP = new ConcurrentLinkedQueue<>();

    private final Bootstrap linkBootstrap;
    private final Runnable tryOpenStreams;
    private final Runnable tryOpenStreamsUDP;
    private final Channel udpChannel;
    private final String identifier;

    public SinglePoint(final SocketAddress bindAddress, final SocketAddress remoteAddress, final Supplier<ProxyHandler> proxySupplier, final SslContext sslCtx, final UUID targetAddress, final Optional<byte[]> secret, final Optional<byte[]> secret_3)
    {
        this(bindAddress, remoteAddress, proxySupplier, sslCtx, targetAddress, secret, secret_3, DEFAULT_NUMLINKS, DEFAULT_LIMITOPEN, DEFAULT_MAXCLF, "SinglePoint");
    }

    public SinglePoint(final SocketAddress bindAddress, final SocketAddress remoteAddress, final Supplier<ProxyHandler> proxySupplier, final SslContext sslCtx, final UUID targetAddress, final Optional<byte[]> secret, final Optional<byte[]> secret_3, final int numLinks, final int limitOpen, final int maxCLF, final String name)
    {
        if (bindAddress == null || remoteAddress == null || proxySupplier == null || sslCtx == null || targetAddress == null || secret == null || secret_3 == null || name == null)
            throw new NullPointerException();
        if (!sslCtx.isClient() || (!secret.isPresent() && !secret_3.isPresent()) || numLinks < 1 || limitOpen < 1 || maxCLF < 0 || name.isEmpty())
            throw new IllegalArgumentException();

        linkBootstrap = new Bootstrap().group(Shared.NettyObjects.workerGroup).channel(Shared.NettyObjects.classSocketChannel).handler(new ChannelInitializer<SocketChannel>()
        {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception
            {
                ch.pipeline().addLast(new IdleStateHandler(0, 0, 60));
                ch.pipeline().addLast(new WriteTimeoutHandler(30));
                ch.pipeline().addLast(new ChunkedWriteHandler());
                ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                ch.pipeline().addLast(proxySupplier.get());
                ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast(new FrameCodec());
                ch.pipeline().addLast(MessageCodec.DEFAULT);
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter()
                {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        channel.attr(ONGOINGSTREAMS_KEY).set(Collections.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>()));
                        channel.attr(PENDINGOPEN_KEY).set(new AtomicInteger());
                        channel.attr(CONNECTIONLATENCYFACTOR_KEY).set(Optional.empty());
                        channel.attr(CONNECTIONLATENCYFACTORCALCULATOR_KEY).set(Optional.empty());
                        channel.attr(AUTHSTATUS_KEY).set(false);
                        channel.attr(CHALLENGE_KEY).set(false);

                        allChannels.add(channel);

                        if (secret_3.isPresent() && Shared.isSHA3Available())
                            channel.writeAndFlush(new Message()
                                    .setType(MessageType.AuthReq_3)
                                    .setPayload(Unpooled.EMPTY_BUFFER));
                        else if (secret.isPresent())
                            channel.writeAndFlush(new Message()
                                    .setType(MessageType.AuthReq)
                                    .setPayload(Unpooled.EMPTY_BUFFER));
                        else
                            channel.close();

                        // use closeFuture as channelInactive might be suppressed by ProxyHandler
                        channel.closeFuture().addListener(future -> {
                            if (!channel.attr(AUTHSTATUS_KEY).get())
                                pendingLinks.updateAndGet(decrementIfPositive);
                        });
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        channel.attr(ONGOINGSTREAMS_KEY).get().forEach(streamId -> {
                            Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                            Optional.ofNullable(inboundStreamsUDP.remove(streamId)).ifPresent(udpChannel.pipeline()::fireUserEventTriggered);
                            outboundStreams.remove(streamId);
                        });
                    }

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
                    {
                        if (msg instanceof Message)
                        {
                            Message cast = (Message) msg;

                            try
                            {
                                handleMessage(ctx, cast);
                            }
                            finally
                            {
                                ReferenceCountUtil.release(cast.getPayload());
                            }
                        }
                        else
                        {
                            try
                            {
                                ctx.close();
                            }
                            finally
                            {
                                ReferenceCountUtil.release(msg);
                            }
                        }
                    }

                    @Override
                    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();
                        final boolean writable = channel.isWritable();

                        channel.attr(ONGOINGSTREAMS_KEY).get().forEach(streamId -> {
                            Optional.ofNullable(inboundStreams.get(streamId)).ifPresent(stream -> stream.config().setAutoRead(writable));
                            if (writable && inboundStreamsUDP.containsKey(streamId))
                                udpChannel.pipeline().fireUserEventTriggered(streamId);
                        });
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        if (channel.attr(CHANNELERROR_KEY).get() != null || !channel.attr(CHANNELERROR_KEY).compareAndSet(null, cause))
                            channel.attr(CHANNELERROR_KEY).get().addSuppressed(cause);

                        channel.close();
                    }

                    private void handleMessage(ChannelHandlerContext ctx, Message msg) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        if (channel.isActive())
                        {
                            switch (msg.getType())
                            {
                                case Ping:
                                {
                                    if (channel.attr(AUTHSTATUS_KEY).get())
                                    {
                                        if (completeConnectionLatencyFactorCalculator.apply(channel).map(clf -> (Boolean) (clf > maxCLF)).orElse(true) && channel.attr(ONGOINGSTREAMS_KEY).get().isEmpty())
                                            channel.close();
                                        else
                                        {
                                            if (pendingStreams.peek() != null)
                                                channel.eventLoop().submit(tryOpenStreams);
                                            if (pendingStreamsUDP.peek() != null)
                                                channel.eventLoop().submit(tryOpenStreamsUDP);
                                        }
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case Open:
                                {
                                    if (channel.attr(AUTHSTATUS_KEY).get())
                                    {
                                        if (completeConnectionLatencyFactorCalculator.apply(channel).map(clf -> (Boolean) (clf > maxCLF)).orElse(false))
                                        {
                                            if (channel.attr(ONGOINGSTREAMS_KEY).get().isEmpty())
                                                channel.close();
                                            else
                                            {
                                                final UUID streamId = msg.getStreamId();

                                                channel.writeAndFlush(new Message()
                                                        .setType(MessageType.Drop)
                                                        .setStreamId(streamId));
                                                channel.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                                            }

                                            if (pendingStreams.peek() != null)
                                                channel.eventLoop().submit(tryOpenStreams);
                                        }
                                        else
                                        {
                                            final UUID streamId = msg.getStreamId();

                                            final Channel pending = pendingStreams.poll();
                                            if (pending != null && pending.isActive())
                                            {
                                                channel.attr(ONGOINGSTREAMS_KEY).get().add(streamId);
                                                outboundStreams.put(streamId, channel);
                                                inboundStreams.put(streamId, pending);
                                                pending.pipeline().fireUserEventTriggered(streamId);
                                            }
                                            else
                                            {
                                                channel.writeAndFlush(new Message()
                                                        .setType(MessageType.Drop)
                                                        .setStreamId(streamId));
                                            }
                                            channel.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                                        }
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case Data:
                                {
                                    if (channel.attr(AUTHSTATUS_KEY).get())
                                    {
                                        final UUID streamId = msg.getStreamId();

                                        if (channel.attr(ONGOINGSTREAMS_KEY).get().contains(streamId))
                                        {
                                            final Channel toSend = inboundStreams.get(streamId);
                                            final InetSocketAddress toSendUDP = inboundStreamsUDP.get(streamId);
                                            if (outboundStreams.get(streamId) == channel && (toSend != null || toSendUDP != null))
                                            {
                                                final ByteBuf payload = msg.getPayload();

                                                if (toSend != null)
                                                    toSend.writeAndFlush(payload.retainedDuplicate());
                                                else if (toSendUDP != null)
                                                    udpChannel.writeAndFlush(new DatagramPacket(payload.retainedDuplicate(), toSendUDP));
                                            }
                                            else if (channel.attr(ONGOINGSTREAMS_KEY).get().remove(streamId))
                                            {
                                                Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                                                Optional.ofNullable(inboundStreamsUDP.remove(streamId)).ifPresent(udpChannel.pipeline()::fireUserEventTriggered);
                                                outboundStreams.remove(streamId);

                                                channel.writeAndFlush(new Message()
                                                        .setType(MessageType.Drop)
                                                        .setStreamId(streamId));
                                            }
                                        }
                                        else
                                        {
                                            channel.writeAndFlush(new Message()
                                                    .setType(MessageType.Drop)
                                                    .setStreamId(streamId));
                                        }
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case Drop:
                                {
                                    if (channel.attr(AUTHSTATUS_KEY).get())
                                    {
                                        final UUID streamId = msg.getStreamId();

                                        if (channel.attr(ONGOINGSTREAMS_KEY).get().remove(streamId))
                                        {
                                            Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                                            Optional.ofNullable(inboundStreamsUDP.remove(streamId)).ifPresent(udpChannel.pipeline()::fireUserEventTriggered);
                                            outboundStreams.remove(streamId);
                                        }
                                        else if (targetAddress.equals(streamId))
                                        {
                                            if (completeConnectionLatencyFactorCalculator.apply(channel).map(clf -> (Boolean) (clf > maxCLF)).orElse(false) && channel.attr(ONGOINGSTREAMS_KEY).get().isEmpty())
                                                channel.close();
                                            else
                                                channel.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                                        }
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case OpenUDP:
                                {
                                    if (channel.attr(AUTHSTATUS_KEY).get())
                                    {
                                        if (completeConnectionLatencyFactorCalculator.apply(channel).map(clf -> (Boolean) (clf > maxCLF)).orElse(false))
                                        {
                                            if (channel.attr(ONGOINGSTREAMS_KEY).get().isEmpty())
                                                channel.close();
                                            else
                                            {
                                                final UUID streamId = msg.getStreamId();

                                                channel.writeAndFlush(new Message()
                                                        .setType(MessageType.Drop)
                                                        .setStreamId(streamId));
                                                channel.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                                            }

                                            if (pendingStreamsUDP.peek() != null)
                                                channel.eventLoop().submit(tryOpenStreamsUDP);
                                        }
                                        else
                                        {
                                            final UUID streamId = msg.getStreamId();

                                            final InetSocketAddress pending = pendingStreamsUDP.poll();
                                            if (pending != null && udpChannel.isActive())
                                            {
                                                channel.attr(ONGOINGSTREAMS_KEY).get().add(streamId);
                                                outboundStreams.put(streamId, channel);
                                                inboundStreamsUDP.put(streamId, pending);
                                                udpChannel.pipeline().fireUserEventTriggered(streamId);
                                            }
                                            else
                                            {
                                                channel.writeAndFlush(new Message()
                                                        .setType(MessageType.Drop)
                                                        .setStreamId(streamId));
                                            }
                                            channel.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                                        }
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case Auth:
                                {
                                    if (channel.attr(CHALLENGE_KEY).get() && channel.attr(AUTHSTATUS_KEY).compareAndSet(false, true))
                                    {
                                        if (completeConnectionLatencyFactorCalculator.apply(channel).map(clf -> (Boolean) (clf > maxCLF)).orElse(true))
                                        {
                                            channel.close();
                                            pendingLinks.updateAndGet(decrementIfPositive);
                                        }
                                        else
                                        {
                                            links.add(channel);
                                            pendingLinks.updateAndGet(decrementIfPositive);

                                            System.out.println(String.format("%s > [%s] link %s up.",
                                                    Shared.printNow(),
                                                    identifier,
                                                    String.format("%s%s", channel.id(), channel.attr(CONNECTIONLATENCYFACTOR_KEY).get().map(f -> String.format("(CLF:%d)", f)).orElse(""))));
                                            channel.closeFuture().addListener(future -> {
                                                channel.eventLoop().submit(() -> {
                                                    System.out.println(String.format("%s > [%s] link %s down.%s%s",
                                                            Shared.printNow(),
                                                            identifier,
                                                            String.format("%s%s", channel.id(), channel.attr(CONNECTIONLATENCYFACTOR_KEY).get().map(f -> String.format("(CLF:%d)", f)).orElse("")),
                                                            channel.attr(ONGOINGSTREAMS_KEY).get().size() > 0 ? String.format(" (%d dropped)", channel.attr(ONGOINGSTREAMS_KEY).get().size()) : "",
                                                            channel.attr(CHANNELERROR_KEY).get() != null ? String.format(" (%s)", channel.attr(CHANNELERROR_KEY).get().toString()) : ""));
                                                });
                                            });

                                            if (pendingStreams.peek() != null)
                                                channel.eventLoop().submit(tryOpenStreams);
                                            if (pendingStreamsUDP.peek() != null)
                                                channel.eventLoop().submit(tryOpenStreamsUDP);
                                        }
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case AuthReq:
                                {
                                    if (!channel.attr(AUTHSTATUS_KEY).get() && channel.attr(CHALLENGE_KEY).compareAndSet(false, true) && secret.isPresent())
                                    {
                                        final ByteBuf payload = msg.getPayload();

                                        channel.writeAndFlush(new Message()
                                                .setType(MessageType.Auth)
                                                .setPayload(Unpooled.wrappedBuffer(Shared.digestSHA256(2, secret.get(), ByteBufUtil.getBytes(payload, payload.readerIndex(), payload.readableBytes(), false)))))
                                                .addListener(future -> {
                                                    if (future.isSuccess())
                                                        initiateConnectionLatencyFactorCalculator.accept(channel);
                                                });
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                case AuthReq_3:
                                {
                                    if (!channel.attr(AUTHSTATUS_KEY).get() && channel.attr(CHALLENGE_KEY).compareAndSet(false, true) && secret_3.isPresent() && Shared.isSHA3Available())
                                    {
                                        final ByteBuf payload = msg.getPayload();

                                        channel.writeAndFlush(new Message()
                                                .setType(MessageType.Auth)
                                                .setPayload(Unpooled.wrappedBuffer(Shared.digestSHA256_3(2, secret_3.get(), ByteBufUtil.getBytes(payload, payload.readerIndex(), payload.readableBytes(), false)))))
                                                .addListener(future -> {
                                                    if (future.isSuccess())
                                                        initiateConnectionLatencyFactorCalculator.accept(channel);
                                                });
                                    }
                                    else
                                    {
                                        channel.close();
                                    }
                                    break;
                                }
                                default:
                                {
                                    channel.close();
                                    break;
                                }
                            }
                        }
                    }

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
                    {
                        if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.ALL_IDLE)
                        {
                            final Channel channel = ctx.channel();

                            if (channel.attr(AUTHSTATUS_KEY).get())
                            {
                                channel.writeAndFlush(new Message()
                                        .setType(MessageType.Ping))
                                        .addListener(future -> {
                                            if (future.isSuccess())
                                                initiateConnectionLatencyFactorCalculator.accept(channel);
                                        });
                            }
                            else
                            {
                                channel.close();
                            }
                        }
                    }

                });
            }

        }).option(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.TCP_NODELAY, true).remoteAddress(remoteAddress);
        tryOpenStreams = new Runnable()
        {

            private final Message msg = new Message()
                    .setType(MessageType.Open)
                    .setStreamId(targetAddress);

            @Override
            public void run()
            {
                if (!links.isEmpty() && pendingStreams.peek() != null)
                {
                    links.stream().sorted(linkSorter).limit(limitOpen).forEach(link -> {
                        link.attr(PENDINGOPEN_KEY).get().incrementAndGet();
                        link.writeAndFlush(msg).addListener(future -> {
                            if (future.isSuccess())
                                initiateConnectionLatencyFactorCalculator.accept(link);
                            else
                                link.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                        });
                    });
                }
            }

        };
        tryOpenStreamsUDP = new Runnable()
        {

            private final Message msg = new Message()
                    .setType(MessageType.OpenUDP)
                    .setStreamId(targetAddress);

            @Override
            public void run()
            {
                if (!links.isEmpty() && pendingStreamsUDP.peek() != null)
                {
                    links.stream().sorted(linkSorter).limit(limitOpen).forEach(link -> {
                        link.attr(PENDINGOPEN_KEY).get().incrementAndGet();
                        link.writeAndFlush(msg).addListener(future -> {
                            if (future.isSuccess())
                                initiateConnectionLatencyFactorCalculator.accept(link);
                            else
                                link.attr(PENDINGOPEN_KEY).get().updateAndGet(decrementIfPositive);
                        });
                    });
                }
            }

        };
        identifier = name;
        GlobalEventExecutor.INSTANCE.scheduleWithFixedDelay(new Runnable()
        {

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
                return (links.size() + pendingLinks.get()) < numLinks && !Shared.NettyObjects.workerGroup.isShuttingDown();
            }

            private void tryConnect()
            {
                if (tryIncrementPending())
                {
                    linkBootstrap.connect().addListener(future -> {
                        if (!future.isSuccess())
                            pendingLinks.updateAndGet(decrementIfPositive);
                    });
                }
            }

            private boolean tryIncrementPending()
            {
                while (true)
                {
                    final int limit = numLinks - links.size();
                    final int pending = pendingLinks.get();

                    if (pending >= limit)
                        return false;

                    if (pendingLinks.compareAndSet(pending, pending + 1))
                        return true;
                }
            }

        }, 1L, 1L, TimeUnit.SECONDS);
        allChannels.add(new ServerBootstrap().group(Shared.NettyObjects.bossGroup, Shared.NettyObjects.workerGroup).channel(Shared.NettyObjects.classServerSocketChannel).childHandler(new ChannelInitializer<SocketChannel>()
        {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception
            {
                ch.pipeline().addLast(new ChunkedWriteHandler());
                ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter()
                {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        allChannels.add(channel);

                        pendingStreams.offer(channel);
                        tryOpenStreams.run();
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();
                        final UUID streamId = channel.attr(STREAMID_KEY).get();

                        if (streamId != null)
                        {
                            Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                            Optional.ofNullable(inboundStreamsUDP.remove(streamId)).ifPresent(udpChannel.pipeline()::fireUserEventTriggered);
                            Optional.ofNullable(outboundStreams.remove(streamId)).ifPresent(link -> {
                                if (link.attr(ONGOINGSTREAMS_KEY).get().remove(streamId))
                                {
                                    link.writeAndFlush(new Message()
                                            .setType(MessageType.Drop)
                                            .setStreamId(streamId));
                                }
                            });
                        }
                        else
                            pendingStreams.remove(channel);
                    }

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
                    {
                        if (msg instanceof ByteBuf)
                        {
                            ByteBuf cast = (ByteBuf) msg;

                            try
                            {
                                handleMessage(ctx, cast);
                            }
                            finally
                            {
                                ReferenceCountUtil.release(cast);
                            }
                        }
                        else
                        {
                            try
                            {
                                ctx.close();
                            }
                            finally
                            {
                                ReferenceCountUtil.release(msg);
                            }
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        if (channel.attr(CHANNELERROR_KEY).get() != null || !channel.attr(CHANNELERROR_KEY).compareAndSet(null, cause))
                            channel.attr(CHANNELERROR_KEY).get().addSuppressed(cause);

                        channel.close();
                    }

                    private void handleMessage(ChannelHandlerContext ctx, ByteBuf msg) throws Exception
                    {
                        final Channel channel = ctx.channel();
                        final UUID streamId = channel.attr(STREAMID_KEY).get();

                        Optional.ofNullable(outboundStreams.get(streamId)).ifPresent(link -> link.writeAndFlush(new Message()
                                .setType(MessageType.Data)
                                .setStreamId(streamId)
                                .setPayload(msg.retainedDuplicate())));
                    }

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
                    {
                        if (evt instanceof UUID)
                        {
                            final Channel channel = ctx.channel();
                            final UUID streamId = (UUID) evt;

                            channel.attr(STREAMID_KEY).set(streamId);
                            channel.config().setAutoRead(true);
                        }
                    }

                });
            }

        }).option(ChannelOption.SO_BACKLOG, 1024).childOption(ChannelOption.SO_KEEPALIVE, true).childOption(ChannelOption.TCP_NODELAY, true).childOption(ChannelOption.AUTO_READ, false).bind(bindAddress).syncUninterruptibly().channel());
        udpChannel = new Bootstrap().group(Shared.NettyObjects.workerGroup).channel(Shared.NettyObjects.classDatagramChannel).handler(new ChannelInitializer<DatagramChannel>()
        {

            @Override
            protected void initChannel(DatagramChannel ch) throws Exception
            {
                ch.pipeline().addLast(new ChunkedWriteHandler());
                ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
                ch.pipeline().addLast(new ChannelInboundHandlerAdapter()
                {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        channel.attr(STREAMIDUDP_KEY).set(new ConcurrentHashMap<>());
                        channel.attr(PENDINGPAYLOADSUDP_KEY).set(new ConcurrentHashMap<>());

                        allChannels.add(channel);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        channel.attr(STREAMIDUDP_KEY).get().values().forEach(streamId -> {
                            Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                            Optional.ofNullable(inboundStreamsUDP.remove(streamId)).ifPresent(udpChannel.pipeline()::fireUserEventTriggered);
                            Optional.ofNullable(outboundStreams.remove(streamId)).ifPresent(link -> {
                                if (link.attr(ONGOINGSTREAMS_KEY).get().remove(streamId))
                                {
                                    link.writeAndFlush(new Message()
                                            .setType(MessageType.Drop)
                                            .setStreamId(streamId));
                                }
                            });
                        });
                        channel.attr(PENDINGPAYLOADSUDP_KEY).get().values().forEach(pendingPayloads -> {
                            while (!pendingPayloads.isEmpty())
                                ReferenceCountUtil.release(pendingPayloads.poll());
                        });
                        pendingStreamsUDP.clear();
                    }

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
                    {
                        if (msg instanceof DatagramPacket)
                        {
                            DatagramPacket cast = (DatagramPacket) msg;

                            try
                            {
                                handleMessage(ctx, cast);
                            }
                            finally
                            {
                                ReferenceCountUtil.release(cast);
                            }
                        }
                        else
                        {
                            try
                            {
                                ctx.close();
                            }
                            finally
                            {
                                ReferenceCountUtil.release(msg);
                            }
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        if (channel.attr(CHANNELERROR_KEY).get() != null || !channel.attr(CHANNELERROR_KEY).compareAndSet(null, cause))
                            channel.attr(CHANNELERROR_KEY).get().addSuppressed(cause);
                    }

                    private void handleMessage(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        if (channel.isActive())
                        {
                            final InetSocketAddress sender = msg.sender();

                            Optional.ofNullable(channel.attr(STREAMIDUDP_KEY).get().get(sender)).ifPresent(streamId -> {
                                if (!outboundStreams.containsKey(streamId))
                                    channel.attr(STREAMIDUDP_KEY).get().remove(sender);
                            });

                            final UUID streamId = channel.attr(STREAMIDUDP_KEY).get().get(sender);
                            final Queue<ByteBuf> pendingPayloads = channel.attr(PENDINGPAYLOADSUDP_KEY).get().get(sender);

                            if (streamId != null)
                            {
                                Optional.ofNullable(outboundStreams.get(streamId)).ifPresent(link -> {
                                    if (link.isWritable())
                                    {
                                        link.writeAndFlush(new Message()
                                                .setType(MessageType.Data)
                                                .setStreamId(streamId)
                                                .setPayload(msg.content().retainedDuplicate()));
                                    }
                                    else if (pendingPayloads != null)
                                    {
                                        pendingPayloads.offer(msg.content().retainedDuplicate());
                                    }
                                    else
                                    {
                                        channel.attr(PENDINGPAYLOADSUDP_KEY).get().computeIfAbsent(sender, unused -> new ConcurrentLinkedQueue<>()).offer(msg.content().retainedDuplicate());
                                    }
                                });
                            }
                            else if (pendingPayloads != null)
                            {
                                pendingPayloads.offer(msg.content().retainedDuplicate());
                            }
                            else
                            {
                                final AtomicBoolean first = new AtomicBoolean();

                                channel.attr(PENDINGPAYLOADSUDP_KEY).get().computeIfAbsent(sender, unused -> {
                                    first.set(true);
                                    return new ConcurrentLinkedQueue<>();
                                }).offer(msg.content().retainedDuplicate());

                                if (first.get())
                                {
                                    pendingStreamsUDP.offer(sender);
                                    tryOpenStreamsUDP.run();
                                }
                            }
                        }
                    }

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
                    {
                        if (evt instanceof UUID)
                        {
                            final Channel channel = ctx.channel();
                            final UUID streamId = (UUID) evt;
                            final InetSocketAddress boundAddress = inboundStreamsUDP.get(streamId);
                            final Channel link = outboundStreams.get(streamId);

                            channel.attr(STREAMIDUDP_KEY).get().put(boundAddress, streamId);
                            Optional.ofNullable(channel.attr(PENDINGPAYLOADSUDP_KEY).get().remove(boundAddress)).ifPresent(pendingPayloads -> {
                                try
                                {
                                    while (!pendingPayloads.isEmpty())
                                    {
                                        final ByteBuf payload = pendingPayloads.poll();

                                        try
                                        {
                                            link.writeAndFlush(new Message()
                                                    .setType(MessageType.Data)
                                                    .setStreamId(streamId)
                                                    .setPayload(payload.retainedDuplicate()));
                                        }
                                        finally
                                        {
                                            ReferenceCountUtil.release(payload);
                                        }
                                    }
                                }
                                finally
                                {
                                    while (!pendingPayloads.isEmpty())
                                        ReferenceCountUtil.release(pendingPayloads.poll());
                                }
                            });
                        }
                        else if (evt instanceof InetSocketAddress)
                        {
                            final Channel channel = ctx.channel();
                            final InetSocketAddress dropAddress = (InetSocketAddress) evt;

                            channel.attr(STREAMIDUDP_KEY).get().remove(dropAddress);
                            Optional.ofNullable(channel.attr(PENDINGPAYLOADSUDP_KEY).get().remove(dropAddress)).ifPresent(pendingPayloads -> {
                                while (!pendingPayloads.isEmpty())
                                    ReferenceCountUtil.release(pendingPayloads.poll());
                            });
                        }
                    }

                });
            }

        }).bind(bindAddress).syncUninterruptibly().channel();
    }

    public ChannelGroup getChannels()
    {
        return allChannels;
    }

    @Override
    public String toString()
    {
        return identifier;
    }

}
