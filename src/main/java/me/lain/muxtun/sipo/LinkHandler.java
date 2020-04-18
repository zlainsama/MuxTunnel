package me.lain.muxtun.sipo;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;

@Sharable
class LinkHandler extends ChannelDuplexHandler
{

    private final AtomicReference<RandomSession> RS = new AtomicReference<>();
    private final AtomicInteger failCount = new AtomicInteger();
    private final AtomicInteger linksCount = new AtomicInteger();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof Message)
        {
            Message cast = (Message) msg;

            try
            {
                handleMessage(LinkContext.getContext(ctx.channel()), cast);
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
        Vars.ChannelError.accumulate(ctx.channel(), cause);

        ctx.close();
    }

    AtomicInteger getLinksCount()
    {
        return linksCount;
    }

    RandomSession getRandomSession(boolean refresh)
    {
        if (RS.get() == null)
            RS.compareAndSet(null, new RandomSession());
        else if (refresh)
            RS.set(new RandomSession());

        return RS.get();
    }

    private void handleMessage(LinkContext lctx, Message msg) throws Exception
    {
        if (lctx.isActive())
        {
            switch (msg.type())
            {
                case PING:
                {
                    break;
                }
                case JOINSESSION:
                {
                    if (lctx.getSession() == null)
                    {
                        synchronized (RS)
                        {
                            UUID sessionId = msg.getId();

                            if (sessionId != null)
                            {
                                if (getRandomSession(false).getSessionId().equals(sessionId))
                                {
                                    boolean remoteCreated = msg.getBuf().readBoolean();

                                    if (!remoteCreated && lctx.getManager().getSessions().get(sessionId) == null)
                                    {
                                        getRandomSession(failCount.incrementAndGet() % 3 == 0);
                                        lctx.close();
                                    }
                                    else
                                    {
                                        if (remoteCreated)
                                            Optional.ofNullable(lctx.getManager().getSessions().remove(sessionId)).ifPresent(LinkSession::close);
                                        failCount.set(0);

                                        boolean[] created = new boolean[] { false };
                                        LinkSession session = lctx.getManager().getSessions().computeIfAbsent(sessionId, key -> {
                                            created[0] = true;
                                            return new LinkSession(key, lctx.getManager(), Vars.SESSIONS.next(), null);
                                        });
                                        if (session.join(lctx.getChannel()))
                                        {
                                            lctx.setSession(session);

                                            if (created[0])
                                            {
                                                IntStream.range(0, lctx.getManager().getTCPRelayRequests().size()).forEach(i -> session.writeAndFlush(MessageType.OPENSTREAM.create().setId(lctx.getManager().getResources().getTargetAddress())));
                                                IntStream.range(0, lctx.getManager().getUDPRelayRequests().size()).forEach(i -> session.writeAndFlush(MessageType.OPENSTREAMUDP.create().setId(lctx.getManager().getResources().getTargetAddress())));
                                            }
                                        }
                                        else
                                        {
                                            if (created[0])
                                            {
                                                session.scheduledSelfClose(true);
                                            }

                                            lctx.close();
                                        }
                                    }
                                }
                                else
                                {
                                    lctx.close();
                                }
                            }
                            else
                            {
                                RandomSession s = getRandomSession(failCount.incrementAndGet() % 3 == 0);
                                lctx.writeAndFlush(MessageType.JOINSESSION.create().setId(s.getSessionId()).setBuf(Unpooled.wrappedBuffer(s.getChallenge())));
                            }
                        }
                    }
                    else
                    {
                        lctx.close();
                    }
                    break;
                }
                case OPENSTREAM:
                {
                    if (lctx.getSession() != null)
                    {
                        LinkSession session = lctx.getSession();
                        int seq = msg.getSeq();

                        if (session.getFlowControl().inRange(seq))
                            session.getInboundBuffer().computeIfAbsent(seq, key -> ReferenceCountUtil.retain(msg));
                        session.updateReceived(ack -> lctx.writeAndFlush(MessageType.ACKNOWLEDGE.create().setAck(ack)));
                    }
                    else
                    {
                        lctx.close();
                    }
                    break;
                }
                case OPENSTREAMUDP:
                {
                    if (lctx.getSession() != null)
                    {
                        LinkSession session = lctx.getSession();
                        int seq = msg.getSeq();

                        if (session.getFlowControl().inRange(seq))
                            session.getInboundBuffer().computeIfAbsent(seq, key -> ReferenceCountUtil.retain(msg));
                        session.updateReceived(ack -> lctx.writeAndFlush(MessageType.ACKNOWLEDGE.create().setAck(ack)));
                    }
                    else
                    {
                        lctx.close();
                    }
                    break;
                }
                case CLOSESTREAM:
                {
                    if (lctx.getSession() != null)
                    {
                        LinkSession session = lctx.getSession();
                        int seq = msg.getSeq();

                        if (session.getFlowControl().inRange(seq))
                            session.getInboundBuffer().computeIfAbsent(seq, key -> ReferenceCountUtil.retain(msg));
                        session.updateReceived(ack -> lctx.writeAndFlush(MessageType.ACKNOWLEDGE.create().setAck(ack)));
                    }
                    else
                    {
                        lctx.close();
                    }
                    break;
                }
                case DATASTREAM:
                {
                    if (lctx.getSession() != null)
                    {
                        LinkSession session = lctx.getSession();
                        int seq = msg.getSeq();

                        if (session.getFlowControl().inRange(seq))
                            session.getInboundBuffer().computeIfAbsent(seq, key -> ReferenceCountUtil.retain(msg));
                        session.updateReceived(ack -> lctx.writeAndFlush(MessageType.ACKNOWLEDGE.create().setAck(ack)));
                    }
                    else
                    {
                        lctx.close();
                    }
                    break;
                }
                case ACKNOWLEDGE:
                {
                    if (lctx.getSession() != null)
                    {
                        lctx.getRTTM().complete().ifPresent(lctx.getSRTT()::updateAndGet);
                        lctx.scheduledMeasurementTimeoutUpdater(false);
                        LinkSession session = lctx.getSession();
                        int ack = msg.getAck();

                        session.acknowledge(ack);
                    }
                    else
                    {
                        lctx.close();
                    }
                    break;
                }
                default:
                {
                    lctx.close();
                    break;
                }
            }
        }
    }

    private void onMessageWrite(LinkContext lctx, Message msg, ChannelPromise promise) throws Exception
    {
        switch (msg.type())
        {
            case OPENSTREAM:
            case OPENSTREAMUDP:
            case CLOSESTREAM:
            case DATASTREAM:
                promise.addListener(future -> {
                    if (future.isSuccess())
                    {
                        if (lctx.getRTTM().initiate())
                            lctx.scheduledMeasurementTimeoutUpdater(true);
                    }
                });
                break;
            default:
                break;
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        if (msg instanceof Message)
            onMessageWrite(LinkContext.getContext(ctx.channel()), (Message) msg, promise);

        ctx.write(msg, promise);
    }

}
