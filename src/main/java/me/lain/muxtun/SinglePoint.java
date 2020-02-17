package me.lain.muxtun;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
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

    private static final AttributeKey<Set<UUID>> ONGOINGSTREAMS_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#OngoingStreams");
    private static final AttributeKey<UUID> STREAMID_KEY = AttributeKey.newInstance("me.lain.muxtun.SinglePoint#StreamId");

    private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final ChannelGroup links = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final Map<UUID, Channel> inboundStreams = new ConcurrentHashMap<>();
    private final Map<UUID, Channel> outboundStreams = new ConcurrentHashMap<>();
    private final Deque<Channel> pendingStreams = new ConcurrentLinkedDeque<>();

    private final Bootstrap linkBootstrap;
    private final Runnable tryOpenStreams;
    private final String identifier;

    public SinglePoint(final SocketAddress bindAddress, final SocketAddress remoteAddress, final Supplier<ProxyHandler> proxySupplier, final SslContext sslCtx, final UUID targetAddress)
    {
        this(bindAddress, remoteAddress, proxySupplier, sslCtx, targetAddress, DEFAULT_NUMLINKS, DEFAULT_LIMITOPEN, "SinglePoint");
    }

    public SinglePoint(final SocketAddress bindAddress, final SocketAddress remoteAddress, final Supplier<ProxyHandler> proxySupplier, final SslContext sslCtx, final UUID targetAddress, final int numLinks, final int limitOpen, final String name)
    {
        if (bindAddress == null || remoteAddress == null || proxySupplier == null || sslCtx == null || targetAddress == null || name == null)
            throw new NullPointerException();
        if (!sslCtx.isClient() || numLinks < 1 || limitOpen < 1 || name.isEmpty())
            throw new IllegalArgumentException();

        linkBootstrap = new Bootstrap().group(Shared.workerGroup).channel(Shared.classSocketChannel).handler(new ChannelInitializer<SocketChannel>()
        {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception
            {
                ch.pipeline().addLast(new FlushConsolidationHandler());
                ch.pipeline().addLast(new IdleStateHandler(0, 0, 10));
                ch.pipeline().addLast(new WriteTimeoutHandler(5));
                ch.pipeline().addLast(proxySupplier.get());
                ch.pipeline().addLast(sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast(new FrameCodec());
                ch.pipeline().addLast(new MessageCodec());
                ch.pipeline().addLast(new SimpleChannelInboundHandler<Message>()
                {

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        channel.attr(ONGOINGSTREAMS_KEY).compareAndSet(null, Collections.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>()));

                        allChannels.add(channel);
                        links.add(channel);

                        System.out.println(String.format("%s > [%s] link %s up.", Shared.printNow(), identifier, channel.id()));

                        ctx.writeAndFlush(new Message()
                                .setType(MessageType.Ping));

                        tryOpenStreams.run();
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        channel.attr(ONGOINGSTREAMS_KEY).get().stream().forEach(streamId -> {
                            Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                            outboundStreams.remove(streamId);
                        });

                        System.out.println(String.format("%s > [%s] link %s down. (%d ongoing streams dropped)", Shared.printNow(), identifier, channel.id(), channel.attr(ONGOINGSTREAMS_KEY).get().size()));
                    }

                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception
                    {
                        final Channel channel = ctx.channel();

                        try
                        {
                            switch (msg.getType())
                            {
                                case Ping:
                                {
                                    tryOpenStreams.run();

                                    break;
                                }
                                case Open:
                                {
                                    if (channel.isActive())
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
                                            ctx.writeAndFlush(new Message()
                                                    .setType(MessageType.Drop)
                                                    .setStreamId(streamId));
                                        }
                                    }

                                    break;
                                }
                                case Data:
                                {
                                    final UUID streamId = msg.getStreamId();

                                    if (channel.attr(ONGOINGSTREAMS_KEY).get().contains(streamId))
                                    {
                                        final Channel toSend = inboundStreams.get(streamId);
                                        if (outboundStreams.get(streamId) == channel && toSend != null)
                                        {
                                            toSend.writeAndFlush(msg.getPayload().retainedDuplicate());
                                        }
                                        else if (channel.attr(ONGOINGSTREAMS_KEY).get().remove(streamId))
                                        {
                                            Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                                            outboundStreams.remove(streamId);

                                            ctx.writeAndFlush(new Message()
                                                    .setType(MessageType.Drop)
                                                    .setStreamId(streamId));
                                        }
                                    }
                                    else
                                    {
                                        ctx.writeAndFlush(new Message()
                                                .setType(MessageType.Drop)
                                                .setStreamId(streamId));
                                    }

                                    break;
                                }
                                case Drop:
                                {
                                    final UUID streamId = msg.getStreamId();

                                    if (channel.attr(ONGOINGSTREAMS_KEY).get().remove(streamId))
                                    {
                                        Optional.ofNullable(inboundStreams.remove(streamId)).ifPresent(Channel::close);
                                        outboundStreams.remove(streamId);
                                    }

                                    break;
                                }
                                default:
                                {
                                    ctx.close();
                                    break;
                                }
                            }
                        }
                        finally
                        {
                            ReferenceCountUtil.release(msg.getPayload());
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
                    {
                        ctx.close();
                    }

                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
                    {
                        if (evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == IdleState.ALL_IDLE)
                            ctx.writeAndFlush(new Message()
                                    .setType(MessageType.Ping));
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
                    links.stream().sorted(Comparator.comparingInt(link -> link.attr(ONGOINGSTREAMS_KEY).get().size())).limit(limitOpen).forEach(link -> link.writeAndFlush(msg));
            }

        };
        identifier = name;
        GlobalEventExecutor.INSTANCE.scheduleWithFixedDelay(new Runnable()
        {

            private final AtomicInteger pendingLinks = new AtomicInteger();

            @Override
            public void run()
            {
                if (shouldTry())
                    tryConnect();
            }

            private boolean shouldTry()
            {
                return (links.size() + pendingLinks.get()) < numLinks && !Shared.workerGroup.isShuttingDown();
            }

            private void tryConnect()
            {
                if (links.size() + pendingLinks.getAndIncrement() < numLinks)
                    linkBootstrap.connect().addListener(future -> pendingLinks.decrementAndGet());
                else
                    pendingLinks.decrementAndGet();
            }

        }, 1L, 1L, TimeUnit.SECONDS);
        allChannels.add(new ServerBootstrap().group(Shared.bossGroup, Shared.workerGroup).channel(Shared.classServerSocketChannel).childHandler(new ChannelInitializer<SocketChannel>()
        {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception
            {
                ch.pipeline().addLast(new FlushConsolidationHandler());
                ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>()
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
                            inboundStreams.remove(streamId);
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
                    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception
                    {
                        final Channel channel = ctx.channel();
                        final UUID streamId = channel.attr(STREAMID_KEY).get();

                        Optional.ofNullable(outboundStreams.get(streamId)).ifPresent(link -> link.writeAndFlush(new Message()
                                .setType(MessageType.Data)
                                .setStreamId(streamId)
                                .setPayload(msg.retainedDuplicate())));
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
                    {
                        ctx.close();
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
