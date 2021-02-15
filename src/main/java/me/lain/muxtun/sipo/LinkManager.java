package me.lain.muxtun.sipo;

import io.netty.util.concurrent.EventExecutor;
import me.lain.muxtun.codec.Message.MessageType;

import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

class LinkManager {

    static final Comparator<LinkSession> SORTER = Comparator.comparingInt(session -> session.getStreams().size() * 64 - session.getFlowControl().window());

    private final SharedResources resources;
    private final Map<UUID, LinkSession> sessions;
    private final Queue<RelayRequest> tcpRelayRequests;
    private final Queue<RelayRequest> udpRelayRequests;
    private final Queue<PendingOpenDrop> tcpPendingOpenDrops;
    private final Queue<PendingOpenDrop> udpPendingOpenDrops;

    LinkManager(SharedResources resources) {
        this.resources = resources;
        this.sessions = new ConcurrentHashMap<>();
        this.tcpRelayRequests = new ConcurrentLinkedQueue<>();
        this.udpRelayRequests = new ConcurrentLinkedQueue<>();
        this.tcpPendingOpenDrops = new ConcurrentLinkedQueue<>();
        this.udpPendingOpenDrops = new ConcurrentLinkedQueue<>();
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

    Queue<PendingOpenDrop> getTcpPendingOpenDrops() {
        return tcpPendingOpenDrops;
    }

    Queue<PendingOpenDrop> getUdpPendingOpenDrops() {
        return udpPendingOpenDrops;
    }

    RelayRequest newTcpRelayRequest(EventExecutor executor) {
        RelayRequest request = new RelayRequest(executor);

        PendingOpenDrop pendingOpenDrop;
        while ((pendingOpenDrop = getTcpPendingOpenDrops().poll()) != null) {
            if (!pendingOpenDrop.getScheduledDrop().cancel(false))
                continue;
            if (request.trySuccess(pendingOpenDrop.getResult()))
                return request;
            pendingOpenDrop.getDropStream().run();
        }

        if (!getTcpRelayRequests().offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
            getSessions().values().stream().filter(LinkSession::isActive).sorted(SORTER).limit(4L).forEach(session -> session.writeAndFlush(MessageType.OPENSTREAM.create()));
        request.addListener(this::removeCancelledTcpRelayRequests);
        return request;
    }

    RelayRequest newUdpRelayRequest(EventExecutor executor) {
        RelayRequest request = new RelayRequest(executor);

        PendingOpenDrop pendingOpenDrop;
        while ((pendingOpenDrop = getUdpPendingOpenDrops().poll()) != null) {
            if (!pendingOpenDrop.getScheduledDrop().cancel(false))
                continue;
            if (request.trySuccess(pendingOpenDrop.getResult()))
                return request;
            pendingOpenDrop.getDropStream().run();
        }

        if (!getUdpRelayRequests().offer(request))
            request.tryFailure(new IllegalStateException());
        else if (!request.isDone())
            getSessions().values().stream().filter(LinkSession::isActive).sorted(SORTER).limit(4L).forEach(session -> session.writeAndFlush(MessageType.OPENSTREAMUDP.create()));
        request.addListener(this::removeCancelledUdpRelayRequests);
        return request;
    }

    private void removeCancelledTcpRelayRequests(Future<?> future) {
        if (future.isCancelled() && future instanceof RelayRequest)
            getTcpRelayRequests().remove(future);
    }

    private void removeCancelledUdpRelayRequests(Future<?> future) {
        if (future.isCancelled() && future instanceof RelayRequest)
            getUdpRelayRequests().remove(future);
    }

}
