package me.lain.muxtun.sipo;

import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import io.netty.util.concurrent.EventExecutor;
import me.lain.muxtun.codec.Message.MessageType;

class LinkManager
{

    static final Comparator<LinkSession> SORTER = Comparator.comparingInt(session -> session.getStreams().size() * 16 - session.getFlowControl().window());

    private final SharedResources resources;
    private final Map<UUID, LinkSession> sessions;
    private final Queue<RelayRequest> tcpRelayRequests;
    private final Queue<RelayRequest> udpRelayRequests;

    LinkManager(SharedResources resources)
    {
        this.resources = resources;
        this.sessions = new ConcurrentHashMap<>();
        this.tcpRelayRequests = new ConcurrentLinkedQueue<>();
        this.udpRelayRequests = new ConcurrentLinkedQueue<>();
    }

    SharedResources getResources()
    {
        return resources;
    }

    Map<UUID, LinkSession> getSessions()
    {
        return sessions;
    }

    Queue<RelayRequest> getTCPRelayRequests()
    {
        return tcpRelayRequests;
    }

    Queue<RelayRequest> getUDPRelayRequests()
    {
        return udpRelayRequests;
    }

    RelayRequest newTCPRelayRequest(EventExecutor executor)
    {
        RelayRequest request = new RelayRequest(executor);
        if (!getTCPRelayRequests().offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
            getSessions().values().stream().filter(LinkSession::isActive).sorted(SORTER).limit(1).forEach(session -> session.writeAndFlush(MessageType.OPENSTREAM.create().setId(getResources().getTargetAddress())));
        return request;
    }

    RelayRequest newUDPRelayRequest(EventExecutor executor)
    {
        RelayRequest request = new RelayRequest(executor);
        if (!getUDPRelayRequests().offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
            getSessions().values().stream().filter(LinkSession::isActive).sorted(SORTER).limit(1).forEach(session -> session.writeAndFlush(MessageType.OPENSTREAMUDP.create().setId(getResources().getTargetAddress())));
        return request;
    }

}
