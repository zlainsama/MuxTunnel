package me.lain.muxtun.sipo;

import io.netty.channel.ChannelFutureListener;

import java.util.UUID;

class SharedResources {

    private final ChannelFutureListener channelAccumulator;
    private final UUID targetAddress;
    private final int maxCLF;

    SharedResources(ChannelFutureListener channelAccumulator, UUID targetAddress, int maxCLF) {
        this.channelAccumulator = channelAccumulator;
        this.targetAddress = targetAddress;
        this.maxCLF = maxCLF;
    }

    ChannelFutureListener getChannelAccumulator() {
        return channelAccumulator;
    }

    int getMaxCLF() {
        return maxCLF;
    }

    UUID getTargetAddress() {
        return targetAddress;
    }

}
