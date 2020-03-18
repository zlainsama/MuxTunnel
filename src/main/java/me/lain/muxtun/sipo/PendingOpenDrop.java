package me.lain.muxtun.sipo;

import io.netty.util.concurrent.ScheduledFuture;

class PendingOpenDrop
{

    final RelayRequestResult result;
    final Runnable dropStream;
    final ScheduledFuture<?> scheduledDrop;

    PendingOpenDrop(RelayRequestResult result, Runnable dropStream, ScheduledFuture<?> scheduledDrop)
    {
        this.result = result;
        this.dropStream = dropStream;
        this.scheduledDrop = scheduledDrop;
    }

}
