package me.lain.muxtun.sipo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.util.FlowControl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

class LinkSession {

    private static final Logger LOGGER = LogManager.getLogger();

    private final UUID sessionId;
    private final LinkManager manager;
    private final EventExecutor executor;
    private final AtomicBoolean closed;
    private final FlowControl flowControl;
    private final Map<Integer, Message> inboundBuffer;
    private final Map<Integer, Message> outboundBuffer;
    private final Deque<Message> pendingMessages;
    private final Set<Channel> members;
    private final Map<UUID, StreamContext> streams;
    private final Set<UUID> closedStreams;
    private final AtomicInteger timeoutCounter;

    LinkSession(UUID sessionId, LinkManager manager, EventExecutor executor) {
        this.sessionId = sessionId;
        this.manager = manager;
        this.executor = executor;
        this.closed = new AtomicBoolean();
        this.flowControl = new FlowControl();
        this.inboundBuffer = new ConcurrentHashMap<>();
        this.outboundBuffer = new ConcurrentHashMap<>();
        this.pendingMessages = new ConcurrentLinkedDeque<>();
        this.members = new DefaultChannelGroup("SessionMembers", executor);
        this.streams = new ConcurrentHashMap<>();
        this.closedStreams = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.timeoutCounter = new AtomicInteger();
    }

    void acknowledge(int ack, int sack) {
        if (!isActive())
            return;

        if (getExecutor().inEventLoop())
            acknowledge0(ack, sack);
        else
            getExecutor().execute(() -> acknowledge0(ack, sack));
    }

    private void acknowledge0(int ack, int sack) {
        if (getFlowControl().acknowledge(getOutboundBuffer().keySet().stream().mapToInt(Integer::intValue), seq -> {
            Message removed = getOutboundBuffer().remove(seq);

            if (removed != null) {
                try {
                    if (removed.type() == MessageType.DATASTREAM) {
                        StreamContext context = getStreams().get(removed.getId());

                        if (context != null)
                            context.updateQuota(i -> i + removed.getBuf().readableBytes());
                    }
                } finally {
                    ReferenceCountUtil.release(removed);
                }
            }
        }, ack, sack) > 0 && isActive() && !getPendingMessages().isEmpty())
            flush();
    }

    void close() {
        if (getExecutor().inEventLoop())
            close0();
        else
            getExecutor().execute(this::close0);
    }

    private void close0() {
        closed.set(true);

        manager.getSessions().remove(sessionId, this);

        while (!members.isEmpty()) {
            members.removeAll(members.stream().peek(Channel::close).collect(Collectors.toList()));
        }
        while (!streams.isEmpty()) {
            streams.values().removeAll(streams.values().stream().peek(StreamContext::close).collect(Collectors.toList()));
        }
        closedStreams.clear();
        while (!inboundBuffer.isEmpty()) {
            inboundBuffer.values().removeAll(inboundBuffer.values().stream().peek(ReferenceCountUtil::release).collect(Collectors.toList()));
        }
        while (!outboundBuffer.isEmpty()) {
            outboundBuffer.values().removeAll(outboundBuffer.values().stream().peek(ReferenceCountUtil::release).collect(Collectors.toList()));
        }
        while (!pendingMessages.isEmpty()) {
            pendingMessages.removeAll(pendingMessages.stream().peek(ReferenceCountUtil::release).collect(Collectors.toList()));
        }
    }

    boolean drop(Channel channel) {
        return getMembers().remove(channel);
    }

    void flush() {
        if (!isActive())
            return;

        if (getExecutor().inEventLoop())
            flush0();
        else
            getExecutor().execute(this::flush0);
    }

    private void flush0() {
        while (getFlowControl().window() > 0 && !getPendingMessages().isEmpty()) {
            if (!isActive())
                break;

            getFlowControl().tryAdvanceSequence(seq -> {
                Message pending = getPendingMessages().poll();

                if (pending != null) {
                    if (getOutboundBuffer().putIfAbsent(seq, pending.setSeq(seq)) != null) {
                        ReferenceCountUtil.release(pending);
                        throw new Error("OverlappedSequenceId " + seq);
                    }

                    switch (pending.type()) {
//                      case OPENSTREAM:
//                      case OPENSTREAMUDP:
//                      {
//                          UUID id = pending.getId();
//
//                          if (id != null)
//                          {
//                              StreamContext sctx = getStreams().get(id);
//
//                              if (sctx != null)
//                              {
//                                  if (sctx.first().get() && sctx.first().compareAndSet(true, false))
//                                      sctx.lastSeq().set(seq);
//                              }
//                          }
//
//                          break;
//                      }
                        case CLOSESTREAM:
                        case DATASTREAM: {
                            UUID id = pending.getId();

                            if (id != null) {
                                StreamContext sctx = getStreams().get(id);

                                if (sctx != null) {
                                    if (sctx.first().get() && sctx.first().compareAndSet(true, false))
                                        sctx.lastSeq().set(getFlowControl().lastAck() - 1);
                                    pending.setReq(sctx.lastSeq().getAndSet(seq));
                                } else {
                                    pending.setReq(seq - 1);
                                }
                            } else {
                                pending.setReq(seq - 1);
                            }

                            break;
                        }
                        default: {
                            break;
                        }
                    }

                    Runnable task = new Runnable() {

                        long lastRTO = 250L;

                        boolean duplicate(Message msg, Consumer<Message> action) {
                            try {
                                action.accept(msg.copy());

                                return true;
                            } catch (IllegalReferenceCountException ignored) {
                                return false;
                            } catch (Throwable e) {
                                LOGGER.error("error duplicating message", e);

                                return false;
                            }
                        }

                        @Override
                        public void run() {
                            Message msg = getOutboundBuffer().get(seq);

                            if (msg != null && isActive()) {
                                Optional<Channel> link = getMembers().stream().sequential()
                                        .filter(channel -> channel.isActive() && channel.isWritable() && LinkContext.getContext(channel).getSession() == LinkSession.this)
                                        .sorted(LinkContext.SORTER)
                                        .filter(channel -> LinkContext.getContext(channel).getTasks().putIfAbsent(seq, this) == null)
                                        .findFirst();

                                if (link.isPresent()) {
                                    LinkContext context = LinkContext.getContext(link.get());

                                    if (duplicate(msg, context::writeAndFlush)) {
                                        getExecutor().schedule(this, lastRTO = context.getSRTT().rto(), TimeUnit.MILLISECONDS);
                                    } else {
                                        getMembers().stream().map(LinkContext::getContext).map(LinkContext::getTasks).forEach(tasks -> tasks.remove(seq, this));
                                    }
                                } else {
                                    getExecutor().schedule(this, lastRTO, TimeUnit.MILLISECONDS);
                                }
                            } else {
                                getMembers().stream().map(LinkContext::getContext).map(LinkContext::getTasks).forEach(tasks -> tasks.remove(seq, this));
                            }
                        }

                    };

                    if (getExecutor().inEventLoop())
                        task.run();
                    else
                        getExecutor().execute(task);

                    return true;
                }

                return false;
            });
        }
    }

    Set<UUID> getClosedStreams() {
        return closedStreams;
    }

    EventExecutor getExecutor() {
        return executor;
    }

    FlowControl getFlowControl() {
        return flowControl;
    }

    Map<Integer, Message> getInboundBuffer() {
        return inboundBuffer;
    }

    LinkManager getManager() {
        return manager;
    }

    Set<Channel> getMembers() {
        return members;
    }

    Map<Integer, Message> getOutboundBuffer() {
        return outboundBuffer;
    }

    Deque<Message> getPendingMessages() {
        return pendingMessages;
    }

    UUID getSessionId() {
        return sessionId;
    }

    Map<UUID, StreamContext> getStreams() {
        return streams;
    }

    AtomicInteger getTimeoutCounter() {
        return timeoutCounter;
    }

    void handleMessage(Message msg) {
        if (getExecutor().inEventLoop())
            handleMessage0(msg);
        else
            getExecutor().execute(() -> handleMessage0(msg));
    }

    private Message handleMessage0(Message msg) {
        if (msg == null || msg == Vars.PLACEHOLDER)
            return null;

        switch (msg.type()) {
            case OPENSTREAM: {
                UUID streamId = msg.getId();

                if (streamId != null) {
                    if (getClosedStreams().contains(streamId)) {
                        writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId), true);
                        writeAndFlush(MessageType.OPENSTREAM.create());
                    } else {
                        RelayRequest request;
                        while ((request = getManager().getTcpRelayRequests().poll()) != null) {
                            if (!request.setUncancellable())
                                continue;
                            break;
                        }

                        RelayRequestResult result = new RelayRequestResult(this, streamId);
                        if (request == null || request.isDone() || !request.trySuccess(result)) {
                            Runnable dropStream = () -> writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId));
                            ScheduledFuture<?> scheduledDrop = getExecutor().schedule(dropStream, 2L, TimeUnit.SECONDS);

                            if (!getManager().getTcpPendingOpenDrops().offer(new PendingOpenDrop(result, dropStream, scheduledDrop)) && scheduledDrop.cancel(false))
                                dropStream.run();
                        }
                    }
                } else if (!getManager().getTcpRelayRequests().isEmpty()) {
                    writeAndFlush(MessageType.OPENSTREAM.create());
                }

                break;
            }
            case OPENSTREAMUDP: {
                UUID streamId = msg.getId();

                if (streamId != null) {
                    if (getClosedStreams().contains(streamId)) {
                        writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId), true);
                        writeAndFlush(MessageType.OPENSTREAMUDP.create());
                    } else {
                        RelayRequest request;
                        while ((request = getManager().getUdpRelayRequests().poll()) != null) {
                            if (!request.setUncancellable())
                                continue;
                            break;
                        }

                        RelayRequestResult result = new RelayRequestResult(this, streamId);
                        if (request == null || request.isDone() || !request.trySuccess(result)) {
                            Runnable dropStream = () -> writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId));
                            ScheduledFuture<?> scheduledDrop = getExecutor().schedule(dropStream, 2L, TimeUnit.SECONDS);

                            if (!getManager().getUdpPendingOpenDrops().offer(new PendingOpenDrop(result, dropStream, scheduledDrop)) && scheduledDrop.cancel(false))
                                dropStream.run();
                        }
                    }
                } else if (!getManager().getUdpRelayRequests().isEmpty()) {
                    writeAndFlush(MessageType.OPENSTREAMUDP.create());
                }

                break;
            }
            case CLOSESTREAM: {
                UUID streamId = msg.getId();

                if (getClosedStreams().add(streamId)) {
                    getExecutor().schedule(() -> getClosedStreams().remove(streamId), 30L, TimeUnit.SECONDS);
                }

                StreamContext context = getStreams().remove(streamId);
                if (context != null) {
                    context.close();
                }

                getPendingMessages().removeAll(getPendingMessages().stream()
                        .filter(pending -> MessageType.DATASTREAM == pending.type() && streamId.equals(pending.getId()))
                        .peek(ReferenceCountUtil::release)
                        .collect(Collectors.toList()));
                getOutboundBuffer().values().stream()
                        .filter(sending -> MessageType.DATASTREAM == sending.type() && streamId.equals(sending.getId()))
                        .map(Message::getBuf)
                        .forEach(buf -> buf.skipBytes(buf.readableBytes()));

                break;
            }
            case DATASTREAM: {
                UUID streamId = msg.getId();
                ByteBuf payload = msg.getBuf();

                if (!getClosedStreams().contains(streamId)) {
                    StreamContext context = getStreams().get(streamId);
                    if (context != null) {
                        context.writeAndFlush(payload.retain());
                    } else {
                        writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId));
                    }
                }

                break;
            }
            default: {
                break;
            }
        }

        return msg;
    }

    boolean isActive() {
        return !closed.get();
    }

    boolean join(Channel channel) {
        return getMembers().add(channel);
    }

    PayloadWriter newPayloadWriter(StreamContext context) {
        return payload -> {
            try {
                if (!isActive())
                    return false;
                int size = payload.readableBytes();
                writeAndFlush(MessageType.DATASTREAM.create().setId(context.getStreamId()).setBuf(payload.retain()));
                context.updateQuota(i -> i - size);
                return true;
            } finally {
                ReferenceCountUtil.release(payload);
            }
        };
    }

    void tick() {
        if (isActive()) {
            if (getExecutor().inEventLoop())
                tick0();
            else
                getExecutor().execute(this::tick0);
        }
    }

    private void tick0() {
        Set<Channel> members = getMembers();

        if (members.isEmpty()) {
            if (getTimeoutCounter().incrementAndGet() > 60)
                close();
        } else {
            getTimeoutCounter().set(0);
            members.forEach(link -> LinkContext.getContext(link).tick());
        }
    }

    void updateReceived(IntConsumer acknowledger) {
        if (!isActive())
            return;

        if (getExecutor().inEventLoop())
            updateReceived0(acknowledger);
        else
            getExecutor().execute(() -> updateReceived0(acknowledger));
    }

    private void updateReceived0(IntConsumer acknowledger) {
        acknowledger.accept(getFlowControl().updateReceived(getInboundBuffer().keySet().stream().mapToInt(Integer::intValue),
                seq -> ReferenceCountUtil.release(handleMessage0(getInboundBuffer().remove(seq))),
                seq -> ReferenceCountUtil.release(getInboundBuffer().remove(seq)),
                (seq, expect) -> {
                    Message msg = getInboundBuffer().get(seq);

                    if (msg != null && msg != Vars.PLACEHOLDER) {
                        switch (msg.type()) {
                            case OPENSTREAM:
                            case OPENSTREAMUDP: {
                                if (getInboundBuffer().replace(seq, msg, Vars.PLACEHOLDER))
                                    ReferenceCountUtil.release(handleMessage0(msg));

                                break;
                            }
                            case CLOSESTREAM:
                            case DATASTREAM: {
                                if (expect - msg.getReq() > 0 || getInboundBuffer().get(msg.getReq()) == Vars.PLACEHOLDER)
                                    if (getInboundBuffer().replace(seq, msg, Vars.PLACEHOLDER))
                                        ReferenceCountUtil.release(handleMessage0(msg));

                                break;
                            }
                            default: {
                                break;
                            }
                        }
                    }

                    return 0;
                }));
    }

    boolean write(Message msg) {
        return write(msg, false);
    }

    boolean write(Message msg, boolean force) {
        try {
            if (!isActive())
                return false;

            switch (msg.type()) {
                case OPENSTREAM:
                case OPENSTREAMUDP: {
                    getPendingMessages().addFirst(ReferenceCountUtil.retain(msg));
                    return true;
                }
                case CLOSESTREAM: {
                    UUID streamId = msg.getId();

                    if (getClosedStreams().add(streamId)) {
                        getExecutor().schedule(() -> getClosedStreams().remove(streamId), 30L, TimeUnit.SECONDS);
                        getPendingMessages().addLast(ReferenceCountUtil.retain(msg));
                        return true;
                    } else if (force) {
                        getPendingMessages().addLast(ReferenceCountUtil.retain(msg));
                        return true;
                    } else {
                        return false;
                    }
                }
                case DATASTREAM: {
                    UUID streamId = msg.getId();

                    if (!getClosedStreams().contains(streamId) || force) {
                        getPendingMessages().addLast(ReferenceCountUtil.retain(msg));
                        return true;
                    } else {
                        return false;
                    }
                }
                default: {
                    return false;
                }
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    void writeAndFlush(Message msg) {
        writeAndFlush(msg, false);
    }

    void writeAndFlush(Message msg, boolean force) {
        write(msg, force);
        flush();
    }

}
