package me.lain.muxtun.sipo;

import io.netty.util.concurrent.ScheduledFuture;

class PendingOpenDrop {

    private final RelayRequestResult result;
    private final Runnable dropStream;
    private final ScheduledFuture<?> scheduledDrop;

    PendingOpenDrop(RelayRequestResult result, Runnable dropStream, ScheduledFuture<?> scheduledDrop) {
        this.result = result;
        this.dropStream = dropStream;
        this.scheduledDrop = scheduledDrop;
    }

    RelayRequestResult getResult() {
        return result;
    }

    Runnable getDropStream() {
        return dropStream;
    }

    ScheduledFuture<?> getScheduledDrop() {
        return scheduledDrop;
    }

}
