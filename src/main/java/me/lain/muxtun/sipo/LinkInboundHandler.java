package me.lain.muxtun.sipo;

import java.util.Optional;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;

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

    private void handleMessage(ChannelHandlerContext ctx, Message msg) throws Exception
    {
        if (ctx.channel().isActive())
        {
            LinkSession session = ctx.channel().attr(Vars.SESSION_KEY).get();

            switch (msg.getType())
            {
                case Ping:
                {
                    if (session.authStatus.completed)
                    {
                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.Ping, session.clf.completeCalculation().orElse(true)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case Open:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();

                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.Open, streamId, session.clf.completeCalculation().orElse(false)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case Data:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();
                        ByteBuf payload = msg.getPayload();

                        Channel toSend = session.ongoingStreams.get(streamId);
                        if (toSend != null)
                        {
                            if (toSend.isActive())
                            {
                                toSend.writeAndFlush(payload.retain());
                            }
                            else
                            {
                                session.ongoingStreams.remove(streamId);
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
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case Drop:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();

                        Channel toClose = session.ongoingStreams.remove(streamId);
                        if (toClose != null && toClose.isActive())
                        {
                            toClose.close();
                        }
                        else if (session.targetAddress.equals(streamId))
                        {
                            ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.OpenFailed, streamId, session.clf.completeCalculation().orElse(false)));
                        }
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case OpenUDP:
                {
                    if (session.authStatus.completed)
                    {
                        UUID streamId = msg.getStreamId();

                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.OpenUDP, streamId, session.clf.completeCalculation().orElse(false)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case Auth:
                {
                    if (session.authStatus.initiated && !session.authStatus.completed)
                    {
                        session.authStatus.completed = true;

                        ctx.fireUserEventTriggered(new LinkSessionEvent(LinkSessionEventType.Auth, session.clf.completeCalculation().orElse(true)));
                    }
                    else
                    {
                        ctx.close();
                    }
                    break;
                }
                case AuthReq:
                {
                    if (!session.authStatus.initiated && !session.authStatus.completed)
                    {
                        session.authStatus.initiated = true;
                        ByteBuf payload = msg.getPayload();
                        byte[] question = ByteBufUtil.getBytes(payload, payload.readerIndex(), payload.readableBytes(), false);

                        Optional<byte[]> answer = session.challengeGenerator.apply(question);
                        if (answer.isPresent())
                        {
                            ctx.writeAndFlush(new Message()
                                    .setType(MessageType.Auth)
                                    .setPayload(Unpooled.wrappedBuffer(answer.get())));
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
                case AuthReq_3:
                {
                    if (!session.authStatus.initiated && !session.authStatus.completed)
                    {
                        session.authStatus.initiated = true;
                        ByteBuf payload = msg.getPayload();
                        byte[] question = ByteBufUtil.getBytes(payload, payload.readerIndex(), payload.readableBytes(), false);

                        Optional<byte[]> answer = session.challengeGenerator_3.apply(question);
                        if (answer.isPresent())
                        {
                            ctx.writeAndFlush(new Message()
                                    .setType(MessageType.Auth)
                                    .setPayload(Unpooled.wrappedBuffer(answer.get())));
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
                default:
                {
                    ctx.close();
                    break;
                }
            }
        }
    }

}
