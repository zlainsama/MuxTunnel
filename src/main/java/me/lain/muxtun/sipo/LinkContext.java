package me.lain.muxtun.sipo;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.EventExecutor;
import me.lain.muxtun.sipo.config.LinkPath;
import me.lain.muxtun.util.RoundTripTimeMeasurement;
import me.lain.muxtun.util.SmoothedRoundTripTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class LinkContext {

    static final Comparator<Channel> SORTER = Comparator.comparingLong(channel -> {
        LinkContext context = LinkContext.getContext(channel);
        return context.getPriority().get() * 1000L + ((context.getSRTT().get() / 50L) * 50L) + Math.max(5L, context.getSRTT().var()) * (1L + 2L * context.getTasks().size());
    });
    private static final Logger LOGGER = LogManager.getLogger();

    private final LinkManager manager;
    private final Channel channel;
    private final EventExecutor executor;
    private final LinkPath linkPath;
    private final AtomicInteger priority;
    private final RoundTripTimeMeasurement RTTM;
    private final SmoothedRoundTripTime SRTT;
    private final Map<Integer, Runnable> tasks;

    private volatile LinkSession session;

    LinkContext(LinkManager manager, Channel channel, LinkPath linkPath) {
        this.manager = manager;
        this.channel = channel;
        this.executor = channel.eventLoop();
        this.linkPath = linkPath;
        this.priority = new AtomicInteger();
        this.RTTM = new RoundTripTimeMeasurement();
        this.SRTT = new SmoothedRoundTripTime() {

            @Override
            public long updateAndGet(long RTT) {
                long result = super.updateAndGet(RTT);

                int clf = (int) (Math.min(Math.max(0L, result), 1000L) / 125L);
                if (clf > getManager().getResources().getMaxCLF())
                    close().addListener(future -> LOGGER.debug("closed link connection due to exceeding maxCLF"));

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

    LinkPath getLinkPath() {
        return linkPath;
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
                getExecutor().execute(this::tick0);
        }
    }

    private void tick0() {
        getRTTM().updateIf(rtt -> rtt >= 2000L).ifPresent(getSRTT()::updateAndGet);
    }

    ChannelFuture writeAndFlush(Object msg) {
        return channel.writeAndFlush(msg);
    }

}
