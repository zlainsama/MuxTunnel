package me.lain.muxtun.sipo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.EventExecutor;
import me.lain.muxtun.Shared;
import me.lain.muxtun.util.RoundTripTimeMeasurement;
import me.lain.muxtun.util.SimpleLogger;
import me.lain.muxtun.util.SmoothedRoundTripTime;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class LinkContext {

    static final Comparator<Channel> SORTER = Comparator.comparingLong(channel -> {
        LinkContext context = LinkContext.getContext(channel);
        return context.getPriority().get() * 1000L + context.getSRTT().get() + context.getSRTT().var() * (1 + 2 * context.getTasks().size());
    });

    private final LinkManager manager;
    private final Channel channel;
    private final EventExecutor executor;
    private final LinkConfig linkConfig;
    private final AtomicInteger priority;
    private final RoundTripTimeMeasurement RTTM;
    private final SmoothedRoundTripTime SRTT;
    private final Map<Integer, Runnable> tasks;

    private volatile LinkSession session;

    LinkContext(LinkManager manager, Channel channel, LinkConfig linkConfig) {
        this.manager = manager;
        this.channel = channel;
        this.executor = channel.eventLoop();
        this.linkConfig = linkConfig;
        this.priority = new AtomicInteger();
        this.RTTM = new RoundTripTimeMeasurement();
        this.SRTT = new SmoothedRoundTripTime() {

            @Override
            public long updateAndGet(long RTT) {
                long result = super.updateAndGet(RTT);

                int clf = (int) (Math.min(Math.max(0L, result), 1000L) / 125L);
                if (clf > getManager().getResources().getMaxCLF())
                    close().addListener(future -> SimpleLogger.println("%s > link connection %s(%s) closed due to exceeding maxCLF. (SRTT:%dms - clf:%d/maxCLF:%d)", Shared.printNow(), getChannel(), getLinkConfig(), result, clf, getManager().getResources().getMaxCLF()));

                return result;
            }

        };
        this.tasks = new ConcurrentHashMap<>();
    }

    static LinkContext getContext(Channel channel) {
        return channel.attr(Vars.LINKCONTEXT_KEY).get();
    }

    ChannelFuture close() {
        return channel.close();
    }

    Channel getChannel() {
        return channel;
    }

    EventExecutor getExecutor() {
        return executor;
    }

    LinkConfig getLinkConfig() {
        return linkConfig;
    }

    LinkManager getManager() {
        return manager;
    }

    AtomicInteger getPriority() {
        return priority;
    }

    RoundTripTimeMeasurement getRTTM() {
        return RTTM;
    }

    LinkSession getSession() {
        return session;
    }

    LinkContext setSession(LinkSession session) {
        this.session = session;
        return this;
    }

    SmoothedRoundTripTime getSRTT() {
        return SRTT;
    }

    Map<Integer, Runnable> getTasks() {
        return tasks;
    }

    boolean isActive() {
        return channel.isActive();
    }

    void tick() {
        if (isActive()) {
            if (getExecutor().inEventLoop())
                tick0();
            else
                getExecutor().execute(() -> tick0());
        }
    }

    private void tick0() {
        getRTTM().updateIf(rtt -> rtt >= 2000L).ifPresent(getSRTT()::updateAndGet);
    }

    ChannelFuture writeAndFlush(Object msg) {
        return channel.writeAndFlush(msg);
    }

}
