package me.lain.muxtun.sipo;

import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import me.lain.muxtun.codec.Message;
import me.lain.muxtun.codec.Message.MessageType;
import me.lain.muxtun.util.FlowControl;

class LinkSession
{

    private final UUID sessionId;
    private final LinkManager manager;
    private final EventExecutor executor;
    private final byte[] challenge;
    private final AtomicBoolean closed;
    private final FlowControl flowControl;
    private final Map<Integer, Message> inboundBuffer;
    private final Map<Integer, Message> outboundBuffer;
    private final Deque<IntFunction<Message>> pendingMessages;
    private final Set<Channel> members;
    private final ChannelFutureListener remover;
    private final AtomicReference<Future<?>> scheduledSelfClose;
    private final Map<UUID, StreamContext> streams;

    LinkSession(UUID sessionId, LinkManager manager, EventExecutor executor, byte[] challenge)
    {
        this.sessionId = sessionId;
        this.manager = manager;
        this.executor = executor;
        this.challenge = challenge;
        this.closed = new AtomicBoolean();
        this.flowControl = new FlowControl();
        this.inboundBuffer = new ConcurrentHashMap<>();
        this.outboundBuffer = new ConcurrentHashMap<>();
        this.pendingMessages = new ConcurrentLinkedDeque<>();
        this.members = Collections.newSetFromMap(new ConcurrentHashMap<Channel, Boolean>());
        this.remover = future -> drop(future.channel());
        this.scheduledSelfClose = new AtomicReference<>();
        this.streams = new ConcurrentHashMap<>();
    }

    void acknowledge(int ack)
    {
        getFlowControl().acknowledge(ack).orElse(IntStream.empty()).forEach(seq -> {
            Optional.ofNullable(getOutboundBuffer().remove(seq)).ifPresent(ReferenceCountUtil::release);
        });

        if (getFlowControl().window() > 0)
            flush();
        if (getFlowControl().window() > 0)
            getStreams().values().forEach(context -> context.windowUpdated(getFlowControl().window()));
    }

    void close()
    {
        if (closed.compareAndSet(false, true))
        {
            manager.getSessions().remove(sessionId, this);

            while (!members.isEmpty())
            {
                members.removeAll(members.stream().peek(Channel::close).collect(Collectors.toList()));
            }
            while (!streams.isEmpty())
            {
                streams.values().removeAll(streams.values().stream().peek(StreamContext::close).collect(Collectors.toList()));
            }
            while (!inboundBuffer.isEmpty())
            {
                inboundBuffer.values().removeAll(inboundBuffer.values().stream().peek(ReferenceCountUtil::release).collect(Collectors.toList()));
            }
            while (!outboundBuffer.isEmpty())
            {
                outboundBuffer.values().removeAll(outboundBuffer.values().stream().peek(ReferenceCountUtil::release).collect(Collectors.toList()));
            }
            while (!pendingMessages.isEmpty())
            {
                IntFunction<Message> pending;
                while ((pending = pendingMessages.poll()) != null)
                    ReferenceCountUtil.release(pending.apply(0));
            }
        }
    }

    boolean drop(Channel channel)
    {
        if (getMembers().remove(channel))
        {
            channel.closeFuture().removeListener(remover);
            scheduledSelfClose(getMembers().isEmpty());
            return true;
        }
        else
        {
            return false;
        }
    }

    synchronized boolean flush()
    {
        if (!isActive())
            return false;

        int flushed = 0;
        while (getFlowControl().window() > 0 && !getPendingMessages().isEmpty())
        {
            if (!isActive())
                break;

            IntFunction<Message> pending = getPendingMessages().peek();

            if (pending != null)
            {
                OptionalInt seq = getFlowControl().tryAdvanceSequence();

                if (seq.isPresent() && getOutboundBuffer().compute(seq.getAsInt(), (key, value) -> {
                    try
                    {
                        return pending.apply(key);
                    }
                    finally
                    {
                        ReferenceCountUtil.release(value);
                    }
                }) != null)
                {
                    flushed += 1;
                    getPendingMessages().remove();

                    getExecutor().submit(new Runnable()
                    {

                        Set<Channel> seen = new HashSet<>();

                        Optional<Message> duplicate(Message msg)
                        {
                            try
                            {
                                switch (msg.type())
                                {
                                    case OPENSTREAM:
                                        return Optional.of(MessageType.OPENSTREAM.create().setSeq(msg.getSeq()).setId(msg.getId()));
                                    case OPENSTREAMUDP:
                                        return Optional.of(MessageType.OPENSTREAMUDP.create().setSeq(msg.getSeq()).setId(msg.getId()));
                                    case CLOSESTREAM:
                                        return Optional.of(MessageType.CLOSESTREAM.create().setSeq(msg.getSeq()).setId(msg.getId()));
                                    case DATASTREAM:
                                        return Optional.of(MessageType.DATASTREAM.create().setSeq(msg.getSeq()).setId(msg.getId()).setBuf(msg.getBuf().retainedDuplicate()));
                                    default:
                                        return Optional.empty();
                                }
                            }
                            catch (Throwable e)
                            {
                                return Optional.empty();
                            }
                        }

                        @Override
                        public void run()
                        {
                            Message msg = getOutboundBuffer().get(seq.getAsInt());

                            if (msg != null)
                            {
                                Optional<Channel> link = getMembers().stream()
                                        .filter(channel -> channel.isActive() && channel.isWritable())
                                        .sorted(LinkContext.SORTER)
                                        .filter(channel -> seen.add(channel))
                                        .findFirst();

                                if (link.isPresent())
                                {
                                    LinkContext context = LinkContext.getContext(link.get());
                                    context.getCount().incrementAndGet();
                                    duplicate(msg).ifPresent(context::writeAndFlush);
                                    getExecutor().schedule(this, context.getSRTT().rto(), TimeUnit.MILLISECONDS);
                                }
                                else
                                {
                                    getExecutor().schedule(this, 1L, TimeUnit.SECONDS);
                                }
                            }
                            else
                            {
                                while (!seen.isEmpty())
                                {
                                    seen.removeAll(seen.stream().peek(channel -> LinkContext.getContext(channel).getCount().updateAndGet(i -> i > 0 ? i - 1 : i)).collect(Collectors.toList()));
                                }
                            }
                        }

                    });
                }
            }
        }

        return flushed > 0;
    }

    byte[] getChallenge()
    {
        return challenge;
    }

    EventExecutor getExecutor()
    {
        return executor;
    }

    FlowControl getFlowControl()
    {
        return flowControl;
    }

    Map<Integer, Message> getInboundBuffer()
    {
        return inboundBuffer;
    }

    LinkManager getManager()
    {
        return manager;
    }

    Set<Channel> getMembers()
    {
        return members;
    }

    Map<Integer, Message> getOutboundBuffer()
    {
        return outboundBuffer;
    }

    Deque<IntFunction<Message>> getPendingMessages()
    {
        return pendingMessages;
    }

    UUID getSessionId()
    {
        return sessionId;
    }

    Map<UUID, StreamContext> getStreams()
    {
        return streams;
    }

    void handleMessage(Message msg) throws Exception
    {
        switch (msg.type())
        {
            case OPENSTREAM:
            {
                UUID streamId = msg.getId();

                if (streamId != null)
                {
                    RelayRequest request;
                    while ((request = getManager().getTCPRelayRequests().poll()) != null)
                    {
                        if (!request.setUncancellable())
                            continue;
                        break;
                    }

                    RelayRequestResult result = new RelayRequestResult(this, streamId);
                    if (request == null || request.isDone() || !request.trySuccess(result))
                    {
                        writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId));
                    }
                }
                else if (!getManager().getTCPRelayRequests().isEmpty())
                {
                    writeAndFlush(MessageType.OPENSTREAM.create().setId(getManager().getResources().getTargetAddress()));
                }
                break;
            }
            case OPENSTREAMUDP:
            {
                UUID streamId = msg.getId();

                if (streamId != null)
                {
                    RelayRequest request;
                    while ((request = getManager().getUDPRelayRequests().poll()) != null)
                    {
                        if (!request.setUncancellable())
                            continue;
                        break;
                    }

                    RelayRequestResult result = new RelayRequestResult(this, streamId);
                    if (request == null || request.isDone() || !request.trySuccess(result))
                    {
                        writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId));
                    }
                }
                else if (!getManager().getUDPRelayRequests().isEmpty())
                {
                    writeAndFlush(MessageType.OPENSTREAM.create().setId(getManager().getResources().getTargetAddress()));
                }
                break;
            }
            case CLOSESTREAM:
            {
                UUID streamId = msg.getId();

                StreamContext context = getStreams().remove(streamId);
                if (context != null)
                {
                    context.close();
                }
                break;
            }
            case DATASTREAM:
            {
                UUID streamId = msg.getId();
                ByteBuf payload = msg.getBuf();

                StreamContext context = getStreams().get(streamId);
                if (context != null)
                {
                    context.writeAndFlush(payload.retain());
                }
                else
                {
                    writeAndFlush(MessageType.CLOSESTREAM.create().setId(streamId));
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }

    boolean isActive()
    {
        return !closed.get();
    }

    boolean join(Channel channel)
    {
        if (getMembers().add(channel))
        {
            channel.closeFuture().addListener(remover);
            scheduledSelfClose(getMembers().isEmpty());
            return true;
        }
        else
        {
            return false;
        }
    }

    PayloadWriter newPayloadWriter(StreamContext context)
    {
        return payload -> {
            try
            {
                if (!isActive())
                    return false;
                writeAndFlush(MessageType.DATASTREAM.create().setId(context.getStreamId()).setBuf(payload.retain()));
                context.windowUpdated(getFlowControl().window());
                return true;
            }
            finally
            {
                ReferenceCountUtil.release(payload);
            }
        };
    }

    void scheduledSelfClose(boolean initiate)
    {
        Optional.ofNullable(scheduledSelfClose.getAndSet(initiate ? getExecutor().schedule(() -> {
            if (getMembers().isEmpty())
                close();
        }, 30L, TimeUnit.SECONDS) : null)).ifPresent(scheduled -> scheduled.cancel(false));
    }

    int updateReceived()
    {
        return getFlowControl().updateReceived(getInboundBuffer().keySet().stream().mapToInt(Integer::intValue), seq -> {
            Optional.ofNullable(getInboundBuffer().remove(seq)).ifPresent(msg -> {
                try
                {
                    handleMessage(msg);
                }
                catch (Throwable e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    ReferenceCountUtil.release(msg);
                }
            });
        }, seq -> {
            Optional.ofNullable(getInboundBuffer().remove(seq)).ifPresent(ReferenceCountUtil::release);
        });
    }

    boolean write(Message msg)
    {
        switch (msg.type())
        {
            case OPENSTREAM:
            case OPENSTREAMUDP:
                getPendingMessages().addFirst(seq -> msg.setSeq(seq));
                return true;
            case CLOSESTREAM:
            case DATASTREAM:
                getPendingMessages().addLast(seq -> msg.setSeq(seq));
                return true;
            default:
                return false;
        }
    }

    void writeAndFlush(Message msg)
    {
        write(msg);
        flush();
    }

}
