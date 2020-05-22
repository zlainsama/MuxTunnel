package me.lain.muxtun.sipo;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message.MessageType;

@Sharable
class UdpStreamHandler extends ChannelInboundHandlerAdapter
{

    private final LinkManager manager;
    private final Map<InetSocketAddress, StreamBridge> bridges;

    UdpStreamHandler(LinkManager manager)
    {
        this.manager = manager;
        this.bridges = new ConcurrentHashMap<>();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof DatagramPacket)
        {
            DatagramPacket cast = (DatagramPacket) msg;

            try
            {
                InetSocketAddress sender = cast.sender();
                boolean[] created = new boolean[] { false };
                StreamBridge bridge = bridges.computeIfAbsent(sender, key -> {
                    created[0] = true;
                    return new StreamBridge(payload -> {
                        try
                        {
                            ctx.writeAndFlush(new DatagramPacket(payload.retain(), key));
                        }
                        finally
                        {
                            ReferenceCountUtil.release(payload);
                        }
                    });
                });

                if (created[0])
                {
                    bridge.newSucceededFuture().addListener(manager.getResources().getChannelAccumulator());
                    bridge.closeFuture().addListener(future -> bridges.remove(sender, bridge));

                    RelayRequest request = manager.newUdpRelayRequest(ctx.channel().eventLoop());
                    if (!request.addListener(future -> {
                        if (future.isSuccess())
                        {
                            RelayRequestResult result = (RelayRequestResult) future.get();
                            StreamContext context = result.getSession().getStreams().compute(result.getStreamId(), (key, value) -> {
                                if (value != null)
                                    throw new Error("BadServer");
                                return new StreamContext(key, result.getSession(), bridge);
                            });
                            bridge.attr(Vars.STREAMCONTEXT_KEY).set(context);
                            bridge.closeFuture().addListener(closeFuture -> {
                                if (context.getSession().getStreams().remove(context.getStreamId(), context))
                                    context.getSession().writeAndFlush(MessageType.CLOSESTREAM.create().setId(context.getStreamId()));
                            });
                            bridge.config().setAutoRead(true);
                        }
                    }).isDone())
                    {
                        ChannelFutureListener taskCancelRequest = future -> request.cancel(false);
                        bridge.closeFuture().addListener(taskCancelRequest);
                        request.addListener(future -> bridge.closeFuture().removeListener(taskCancelRequest));
                    }
                }

                bridge.accept(cast.content().retain());
            }
            finally
            {
                ReferenceCountUtil.release(cast);
            }
        }
        else
        {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
    }

}
