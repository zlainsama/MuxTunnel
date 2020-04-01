package me.lain.muxtun.sipo;

import java.util.Optional;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.codec.SnappyCodec;

@Sharable
class LinkInboundHandler extends ChannelInboundHandlerAdapter
{

    static final LinkInboundHandler DEFAULT = new LinkInboundHandler();

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

    private void handleMessage(ChannelHandlerContext ctx, Message msg) throws Exception
    {
        if (ctx.channel().isActive())
        {
            LinkSession session = ctx.channel().attr(Vars.SESSION_KEY).get();

            switch (msg.type())
            {
                case PING:
                {
                    if (session.authStatus.completed)
                    {
                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.PING, session.clf.completeCalculation().orElse(true)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case OPEN:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();

                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.OPEN, streamId, session.clf.completeCalculation().orElse(false)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case DATA:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();
                        ByteBuf payload = msg.getPayload();

                        StreamContext sctx = session.ongoingStreams.get(streamId);
                        if (sctx != null)
                        {
                            if (sctx.isActive())
                            {
                                int size = payload.readableBytes();
                                sctx.writeAndFlush(payload.retain());
                                sctx.updateReceived(i -> i + size);
                            }
                            else
                            {
                                session.ongoingStreams.remove(streamId);
                                ctx.writeAndFlush(MessageType.DROP.create().setStreamId(streamId));
                            }
                        }
                        else
                        {
                            ctx.writeAndFlush(MessageType.DROP.create().setStreamId(streamId));
                        }
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case DROP:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();

                        StreamContext sctx = session.ongoingStreams.remove(streamId);
                        if (sctx != null && sctx.isActive())
                        {
                            sctx.close();
                        }
                        else if (session.targetAddress.equals(streamId))
                        {
                            ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.OPENFAILED, streamId, session.clf.completeCalculation().orElse(false)));
                        }
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case OPENUDP:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();

                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.OPENUDP, streamId, session.clf.completeCalculation().orElse(false)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case UPDATEWINDOW:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();
                        int increment = msg.getWindowSizeIncrement();

                        StreamContext sctx = session.ongoingStreams.get(streamId);
                        if (sctx != null && sctx.isActive())
                        {
                            if (increment <= 0)
                            {
                                sctx.close();
                            }
                            else
                            {
                                boolean[] wasPositive = new boolean[] { false };
                                if (sctx.updateWindowSize(i -> {
                                    wasPositive[0] = i > 0;
                                    return i += increment;
                                }) < 0 && wasPositive[0])
                                {
                                    sctx.close();
                                }
                            }
                        }
                        else
                        {
                            ctx.writeAndFlush(MessageType.DROP.create().setStreamId(streamId));
                        }
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case AUTH:
                {
                    if (session.authStatus.initiated && !session.authStatus.completed)
                    {
                        session.authStatus.completed = true;

                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.AUTH, session.clf.completeCalculation().orElse(true)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case AUTHREQ:
                {
                    if (!session.authStatus.initiated && !session.authStatus.completed)
                    {
                        session.authStatus.initiated = true;
                        ByteBuf payload = msg.getPayload();
                        byte[] question = ByteBufUtil.getBytes(payload, payload.readerIndex(), payload.readableBytes(), false);

                        Optional<byte[]> answer = session.challengeGenerator.apply(question);
                        if (answer.isPresent())
                        {
                            ctx.writeAndFlush(MessageType.AUTH.create().setPayload(Unpooled.wrappedBuffer(answer.get())));
                        }
                        else
                        {
                            ctx.close();
                        }
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case AUTHREQ3:
                {
                    if (!session.authStatus.initiated && !session.authStatus.completed)
                    {
                        session.authStatus.initiated = true;
                        ByteBuf payload = msg.getPayload();
                        byte[] question = ByteBufUtil.getBytes(payload, payload.readerIndex(), payload.readableBytes(), false);

                        Optional<byte[]> answer = session.challengeGenerator_3.apply(question);
                        if (answer.isPresent())
                        {
                            ctx.writeAndFlush(MessageType.AUTH.create().setPayload(Unpooled.wrappedBuffer(answer.get())));
                        }
                        else
                        {
                            ctx.close();
                        }
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case SNAPPY:
                {
                    if (session.authStatus.completed)
                    {
                        ctx.pipeline().addBefore("FrameCodec", "SnappyCodec", new SnappyCodec());
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case FLOWCONTROL:
                {
                    if (!session.authStatus.completed || !session.flowControl.compareAndSet(false, true))
                    {
                        ctx.close();
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
    }

}
