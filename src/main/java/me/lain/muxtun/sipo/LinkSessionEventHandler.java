package me.lain.muxtun.sipo;

import io.netty.channel.Channel;

@FunctionalInterface
interface LinkSessionEventHandler
{

    void handleEvent(Channel linkChannel, LinkSession session, LinkSessionEvent event) throws Exception;

}
