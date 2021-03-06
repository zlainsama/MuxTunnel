package me.lain.muxtun.sipo;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.handler.proxy.ProxyConnectException;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.sipo.config.LinkPath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

@Sharable
class LinkHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LogManager.getLogger();

    private final AtomicReference<RandomSession> RS = new AtomicReference<>();
    private final AtomicInteger failCount = new AtomicInteger();
    private final Map<Integer, Optional<Channel>> channelMap = new ConcurrentHashMap<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Message) {
            Message cast = (Message) msg;

            try {
                handleMessage(LinkContext.getContext(ctx.channel()), cast);
            } finally {
                ReferenceCountUtil.release(cast);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof ProxyConnectException)
            ctx.close();
        else
            ctx.close().addListener(future -> LOGGER.error("closed link connection due to error", cause));
    }

    Map<Integer, Optional<Channel>> getChannelMap() {
        return channelMap;
    }

    RandomSession getRandomSession(boolean refresh) {
        if (RS.get() == null)
            RS.compareAndSet(null, new RandomSession());
        else if (refresh)
            RS.set(new RandomSession());

        return RS.get();
    }

    private void handleMessage(LinkContext lctx, Message msg) throws Exception {
        if (lctx.isActive()) {
            switch (msg.type()) {
                case PING: {
                    lctx.getSRTT().reset();
                    break;
                }
                case JOINSESSION: {
                    if (lctx.getSession() == null) {
                        synchronized (RS) {
                            UUID sessionId = msg.getId();

                            if (sessionId != null) {
                                if (getRandomSession(false).getSessionId().equals(sessionId)) {
                                    boolean remoteCreated = msg.getBuf().readBoolean();

                                    if (!remoteCreated && lctx.getManager().getSessions().get(sessionId) == null) {
                                        getRandomSession(failCount.incrementAndGet() % 3 == 0);
                                        lctx.close();
                                    } else {
                                        if (remoteCreated)
                                            Optional.ofNullable(lctx.getManager().getSessions().remove(sessionId)).ifPresent(LinkSession::close);
                                        failCount.set(0);

                                        boolean[] created = new boolean[]{false};
                                        LinkSession session = lctx.getManager().getSessions().computeIfAbsent(sessionId, key -> {
                                            created[0] = true;
                                            return new LinkSession(key, lctx.getManager(), Vars.SESSIONS.next());
                                        });
                                        if (session.join(lctx.getChannel())) {
                                            lctx.setSession(session);

                                            LinkPath linkPath = lctx.getLinkPath();
                                            short priority = linkPath.getPriority();
                                            long writeLimit = linkPath.getWriteLimit();
                                            lctx.getPriority().set(priority);
                                            lctx.getChannel().pipeline().addBefore(Vars.HANDLERNAME_TLS, Vars.HANDLERNAME_LIMITER, new ChannelTrafficShapingHandler(writeLimit, 0L));
                                            lctx.writeAndFlush(MessageType.LINKCONFIG.create().setPriority(priority).setWriteLimit(writeLimit));

                                            if (created[0]) {
                                                IntStream.range(0, lctx.getManager().getTcpRelayRequests().size()).forEach(i -> session.writeAndFlush(MessageType.OPENSTREAM.create()));
                                                IntStream.range(0, lctx.getManager().getUdpRelayRequests().size()).forEach(i -> session.writeAndFlush(MessageType.OPENSTREAMUDP.create()));
                                            }
                                        } else {
                                            lctx.close();
                                        }
                                    }
                                } else {
                                    lctx.close();
                                }
                            } else {
                                RandomSession s = getRandomSession(failCount.incrementAndGet() % 3 == 0);
                                lctx.writeAndFlush(MessageType.JOINSESSION.create().setId(s.getSessionId()).setBuf(Unpooled.wrappedBuffer(s.getChallenge())));
                            }
                        }
                    } else {
                        lctx.close();
                    }
                    break;
                }
                case OPENSTREAM:
                case OPENSTREAMUDP:
                case CLOSESTREAM:
                case DATASTREAM: {
                    if (lctx.getSession() != null) {
                        LinkSession session = lctx.getSession();
                        int seq = msg.getSeq();

                        boolean inRange;
                        if (inRange = session.getFlowControl().inRange(seq))
                            session.getInboundBuffer().computeIfAbsent(seq, key -> ReferenceCountUtil.retain(msg));
                        session.updateReceived(ack -> lctx.writeAndFlush(MessageType.ACKNOWLEDGE.create().setAck(ack).setSAck(inRange ? seq : ack - 1)));
                    } else {
                        lctx.close();
                    }
                    break;
                }
                case ACKNOWLEDGE: {
                    if (lctx.getSession() != null) {
                        lctx.getRTTM().complete().ifPresent(lctx.getSRTT()::updateAndGet);
                        LinkSession session = lctx.getSession();
                        int ack = msg.getAck();
                        int sack = msg.getSAck();

                        session.acknowledge(ack, sack);
                    } else {
                        lctx.close();
                    }
                    break;
                }
                case LINKCONFIG: {
                    if (lctx.getSession() != null) {
                        short priority = msg.getPriority();
                        long writeLimit = msg.getWriteLimit();
                        ChannelPipeline p = lctx.getChannel().pipeline();

                        lctx.getPriority().set(priority);
                        if (writeLimit > 0L) {
                            ChannelTrafficShapingHandler limiter = new ChannelTrafficShapingHandler(writeLimit, 0L);
                            if (p.get(Vars.HANDLERNAME_LIMITER) != null) {
                                p.replace(Vars.HANDLERNAME_LIMITER, Vars.HANDLERNAME_LIMITER, limiter);
                            } else {
                                p.addBefore(Vars.HANDLERNAME_TLS, Vars.HANDLERNAME_LIMITER, limiter);
                            }
                        } else if (p.get(Vars.HANDLERNAME_LIMITER) != null) {
                            p.remove(Vars.HANDLERNAME_LIMITER);
                        }
                    } else {
                        lctx.close();
                    }
                    break;
                }
                default: {
                    lctx.close();
                    break;
                }
            }
        }
    }

    private void onMessageWrite(LinkContext lctx, Message msg, ChannelPromise promise) throws Exception {
        switch (msg.type()) {
            case OPENSTREAM:
            case OPENSTREAMUDP:
            case CLOSESTREAM:
            case DATASTREAM: {
                promise.addListener(future -> {
                    if (future.isSuccess())
                        lctx.getRTTM().initiate();
                });
                break;
            }
            default: {
                break;
            }
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Message)
            onMessageWrite(LinkContext.getContext(ctx.channel()), (Message) msg, promise);

        ctx.write(msg, promise);
    }

}
