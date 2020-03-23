package me.lain.muxtun.sipo;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.sipo.StreamContext.DefaultStreamContext;
import me.lain.muxtun.sipo.StreamContext.FlowControlledStreamContext;

@Sharable
class UDPStreamInboundHandler extends ChannelInboundHandlerAdapter
{

    private static class PendingWrite
    {

        final ByteBuf msg;
        final ChannelPromise promise;

        PendingWrite(ByteBuf msg, ChannelPromise promise)
        {
            this.msg = msg;
            this.promise = promise;
        }

    }

    private final ChannelGroup channels;
    private final Function<Channel, RelayRequest> register;
    private final Map<InetSocketAddress, Channel> boundChannels;

    UDPStreamInboundHandler(ChannelGroup channels, Function<Channel, RelayRequest> register)
    {
        this.channels = channels;
        this.register = register;
        this.boundChannels = new ConcurrentHashMap<>();
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
            }
            finally
            {
                ReferenceCountUtil.release(msg);
            }
        }
    }

    private void handleMessage(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception
    {
        InetSocketAddress sender = msg.sender();
        Channel boundChannel = boundChannels.get(sender);

        if (boundChannel != null)
        {
            boundChannel.writeAndFlush(msg.content().retain());
        }
        else
        {
            AtomicReference<Channel> boundLink = new AtomicReference<>();
            AtomicReference<UUID> boundId = new AtomicReference<>();
            AtomicReference<StreamContext> boundSctx = new AtomicReference<>();

            PayloadWriter writerHead = payload -> {
                try
                {
                    if (!ctx.channel().isActive())
                        return false;
                    ctx.writeAndFlush(new DatagramPacket(payload.retain(), sender));
                    return true;
                }
                finally
                {
                    ReferenceCountUtil.release(payload);
                }
            };
            PayloadWriter writerTail = payload -> {
                try
                {
                    Channel link = boundLink.get();
                    if (link == null || !link.isActive())
                        return false;
                    UUID streamId = boundId.get();
                    if (streamId == null)
                        return false;
                    StreamContext sctx = boundSctx.get();
                    if (sctx == null)
                        return false;
                    int size = payload.readableBytes();
                    link.writeAndFlush(MessageType.DATA.create().setStreamId(streamId).setPayload(payload.retain()));
                    sctx.updateWindowSize(i -> i - size);
                    return true;
                }
                finally
                {
                    ReferenceCountUtil.release(payload);
                }
            };

            EmbeddedChannel Head = new EmbeddedChannel();
            EmbeddedChannel Tail = new EmbeddedChannel();

            Queue<PendingWrite> inHead = new ConcurrentLinkedQueue<>();
            Queue<PendingWrite> outHead = new ConcurrentLinkedQueue<>();
            Queue<PendingWrite> inTail = new ConcurrentLinkedQueue<>();
            Queue<PendingWrite> outTail = new ConcurrentLinkedQueue<>();

            Head.closeFuture().addListener(future -> {
                PendingWrite pending;
                while ((pending = inHead.poll()) != null)
                {
                    try
                    {
                        if (!pending.promise.isVoid())
                            pending.promise.tryFailure(new ClosedChannelException());
                    }
                    catch (Exception e)
                    {
                    }
                    finally
                    {
                        pending.msg.release();
                    }
                }
                while ((pending = outHead.poll()) != null)
                {
                    try
                    {
                        if (!pending.promise.isVoid())
                            pending.promise.tryFailure(new ClosedChannelException());
                    }
                    catch (Exception e)
                    {
                    }
                    finally
                    {
                        pending.msg.release();
                    }
                }
            });
            Tail.closeFuture().addListener(future -> {
                PendingWrite pending;
                while ((pending = inTail.poll()) != null)
                {
                    try
                    {
                        if (!pending.promise.isVoid())
                            pending.promise.tryFailure(new ClosedChannelException());
                    }
                    catch (Exception e)
                    {
                    }
                    finally
                    {
                        pending.msg.release();
                    }
                }
                while ((pending = outTail.poll()) != null)
                {
                    try
                    {
                        if (!pending.promise.isVoid())
                            pending.promise.tryFailure(new ClosedChannelException());
                    }
                    catch (Exception e)
                    {
                    }
                    finally
                    {
                        pending.msg.release();
                    }
                }
            });

            Head.pipeline().addLast(new ChannelDuplexHandler()
            {

                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
                {
                    try
                    {
                        if (!writerHead.write(((ByteBuf) msg).retain()))
                            ctx.close();
                    }
                    finally
                    {
                        ReferenceCountUtil.release(msg);
                    }
                }

                @Override
                public void flush(ChannelHandlerContext ctx) throws Exception
                {
                    PendingWrite pending;
                    while ((pending = outHead.poll()) != null)
                    {
                        boolean release = true;

                        try
                        {
                            inTail.add(pending);
                            release = false;
                        }
                        catch (Exception e)
                        {
                            if (!pending.promise.isVoid())
                                pending.promise.tryFailure(e);
                        }
                        finally
                        {
                            if (release)
                                pending.msg.release();
                        }
                    }

                    if (!inTail.isEmpty() && Tail.config().isAutoRead())
                        Tail.read();
                }

                @Override
                public void read(ChannelHandlerContext ctx) throws Exception
                {
                    PendingWrite pending;
                    if ((pending = inHead.poll()) != null)
                    {
                        boolean release = true;

                        try
                        {
                            Head.writeInbound(pending.msg);
                            release = false;
                            if (!pending.promise.isVoid())
                                pending.promise.trySuccess();
                        }
                        catch (Exception e)
                        {
                            if (!pending.promise.isVoid())
                                pending.promise.tryFailure(e);
                        }
                        finally
                        {
                            if (release)
                                pending.msg.release();
                        }
                    }
                }

                @Override
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
                {
                    boolean release = true;

                    try
                    {
                        outHead.add(new PendingWrite((ByteBuf) msg, promise));
                        release = false;
                    }
                    catch (Exception e)
                    {
                        if (!promise.isVoid())
                            promise.tryFailure(e);
                    }
                    finally
                    {
                        if (release)
                            ReferenceCountUtil.release(msg);
                    }
                }

            });
            Tail.pipeline().addLast(new ChannelDuplexHandler()
            {

                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
                {
                    try
                    {
                        if (!writerTail.writeSlices(((ByteBuf) msg).retain(), 65536, null))
                            ctx.close();
                    }
                    finally
                    {
                        ReferenceCountUtil.release(msg);
                    }
                }

                @Override
                public void flush(ChannelHandlerContext ctx) throws Exception
                {
                    PendingWrite pending;
                    while ((pending = outTail.poll()) != null)
                    {
                        boolean release = true;

                        try
                        {
                            inHead.add(pending);
                            release = false;
                        }
                        catch (Exception e)
                        {
                            if (!pending.promise.isVoid())
                                pending.promise.tryFailure(e);
                        }
                        finally
                        {
                            if (release)
                                pending.msg.release();
                        }
                    }

                    if (!inHead.isEmpty() && Head.config().isAutoRead())
                        Head.read();
                }

                @Override
                public void read(ChannelHandlerContext ctx) throws Exception
                {
                    PendingWrite pending;
                    if ((pending = inTail.poll()) != null)
                    {
                        boolean release = true;

                        try
                        {
                            Tail.writeInbound(pending.msg);
                            release = false;
                            if (!pending.promise.isVoid())
                                pending.promise.trySuccess();
                        }
                        catch (Exception e)
                        {
                            if (!pending.promise.isVoid())
                                pending.promise.tryFailure(e);
                        }
                        finally
                        {
                            if (release)
                                pending.msg.release();
                        }
                    }
                }

                @Override
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
                {
                    boolean release = true;

                    try
                    {
                        outTail.add(new PendingWrite((ByteBuf) msg, promise));
                        release = false;
                    }
                    catch (Exception e)
                    {
                        if (!promise.isVoid())
                            promise.tryFailure(e);
                    }
                    finally
                    {
                        if (release)
                            ReferenceCountUtil.release(msg);
                    }
                }

            });

            Head.closeFuture().addListener(future -> Tail.close());
            Tail.closeFuture().addListener(future -> Head.close());

            channels.add(Head);
            channels.add(Tail);

            Head.closeFuture().addListener(future -> boundChannels.remove(sender));
            Tail.config().setAutoRead(false);
            boundChannels.put(sender, Head);
            Head.writeAndFlush(msg.content().retain());

            RelayRequest request = (RelayRequest) register.apply(ctx.channel()).addListener(future -> {
                if (future.isSuccess())
                {
                    RelayRequestResult result = (RelayRequestResult) future.get();

                    boundLink.set(result.linkChannel);
                    boundId.set(result.streamId);
                    boundSctx.set(result.session.flowControl.get() ? new FlowControlledStreamContext(Tail) : new DefaultStreamContext(Tail));
                    result.session.ongoingStreams.put(result.streamId, boundSctx.get());
                    Tail.closeFuture().addListener(closeFuture -> {
                        if (result.linkChannel.isActive() && result.session.ongoingStreams.remove(result.streamId) != null)
                            result.linkChannel.writeAndFlush(MessageType.DROP.create().setStreamId(result.streamId));
                    });
                    Tail.config().setAutoRead(true);
                }
                else
                {
                    Tail.close();
                }
            });

            if (!request.isDone())
            {
                ChannelFutureListener taskCancelRequest = future -> request.cancel(false);
                Tail.closeFuture().addListener(taskCancelRequest);
                request.addListener(future -> Tail.closeFuture().removeListener(taskCancelRequest));
            }
        }
    }

}
