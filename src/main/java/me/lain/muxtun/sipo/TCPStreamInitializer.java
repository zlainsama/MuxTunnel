package me.lain.muxtun.sipo;

import java.util.function.Function;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;

@Sharable
class TCPStreamInitializer extends ChannelInitializer<SocketChannel>
{

    private final ChannelGroup channels;
    private final Function<Channel, RelayRequest> register;

    TCPStreamInitializer(ChannelGroup channels, Function<Channel, RelayRequest> register)
    {
        this.channels = channels;
        this.register = register;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception
    {
        ch.config().setAutoRead(false);
        channels.add(ch);

        ch.pipeline().addLast(new ChunkedWriteHandler());
        ch.pipeline().addLast(new FlushConsolidationHandler(64, true));
        ch.pipeline().addLast(TCPStreamInboundHandler.DEFAULT);
        ch.pipeline().addLast(TCPStreamExceptionHandler.DEFAULT);

        RelayRequest request = (RelayRequest) register.apply(ch).addListener(future -> {
            if (future.isSuccess())
            {
                RelayRequestResult result = (RelayRequestResult) future.get();

                result.session.ongoingStreams.put(result.streamId, ch);
                ch.attr(Vars.WRITER_KEY).set(payload -> {
                    try
                    {
                        if (!result.linkChannel.isActive())
                            return false;
                        result.linkChannel.writeAndFlush(new Message()
                                .setType(MessageType.Data)
                                .setStreamId(result.streamId)
                                .setPayload(payload.retain()));
                        return true;
                    }
                    finally
                    {
                        ReferenceCountUtil.release(payload);
                    }
                });
                ch.closeFuture().addListener(closeFuture -> {
                    if (result.linkChannel.isActive() && result.session.ongoingStreams.remove(result.streamId) == ch)
                        result.linkChannel.writeAndFlush(new Message()
                                .setType(MessageType.Drop)
                                .setStreamId(result.streamId));
                });
                ch.config().setAutoRead(true);
            }
            else
            {
                ch.close();
            }
        });

        if (!request.isDone())
        {
            ChannelFutureListener taskCancelRequest = future -> request.cancel(false);
            ch.closeFuture().addListener(taskCancelRequest);
            request.addListener(future -> ch.closeFuture().removeListener(taskCancelRequest));
        }
    }

}
