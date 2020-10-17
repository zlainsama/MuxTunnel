package me.lain.muxtun.sipo;

import io.netty.util.concurrent.EventExecutor;
import me.lain.muxtun.codec.Message.MessageType;

import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

class LinkManager {

    static final Comparator<LinkSession> SORTER = Comparator.comparingInt(session -> session.getStreams().size() * 64 - session.getFlowControl().window());

    private final SharedResources resources;
    private final Map<UUID, LinkSession> sessions;
    private final Queue<RelayRequest> tcpRelayRequests;
    private final Queue<RelayRequest> udpRelayRequests;

    LinkManager(SharedResources resources) {
        this.resources = resources;
        this.sessions = new ConcurrentHashMap<>();
        this.tcpRelayRequests = new ConcurrentLinkedQueue<>();
        this.udpRelayRequests = new ConcurrentLinkedQueue<>();
    }

    SharedResources getResources() {
        return resources;
    }

    Map<UUID, LinkSession> getSessions() {
        return sessions;
    }

    Queue<RelayRequest> getTcpRelayRequests() {
        return tcpRelayRequests;
    }

    Queue<RelayRequest> getUdpRelayRequests() {
        return udpRelayRequests;
    }

    RelayRequest newTcpRelayRequest(EventExecutor executor) {
        RelayRequest request = new RelayRequest(executor);
        if (!getTcpRelayRequests().offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
            getSessions().values().stream().filter(LinkSession::isActive).sorted(SORTER).limit(2L).forEach(session -> session.writeAndFlush(MessageType.OPENSTREAM.create()));
        return request;
    }

    RelayRequest newUdpRelayRequest(EventExecutor executor) {
        RelayRequest request = new RelayRequest(executor);
        if (!getUdpRelayRequests().offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
            getSessions().values().stream().filter(LinkSession::isActive).sorted(SORTER).limit(2L).forEach(session -> session.writeAndFlush(MessageType.OPENSTREAMUDP.create()));
        return request;
    }

}
