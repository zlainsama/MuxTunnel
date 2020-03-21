package me.lain.muxtun.sipo;

import io.netty.channel.EventLoopGroup;
import io.netty.util.AttributeKey;
import me.lain.muxtun.Shared;

class Vars
{

    static final AttributeKey<Throwable> ERROR_KEY = AttributeKey.newInstance("me.lain.muxtun.sipo.Vars#Error");
    static final AttributeKey<LinkSession> SESSION_KEY = AttributeKey.newInstance("me.lain.muxtun.sipo.Vars#Session");
    static final AttributeKey<PayloadWriter> WRITER_KEY = AttributeKey.newInstance("me.lain.muxtun.sipo.Vars#Writer");

    static final EventLoopGroup BOSS = Shared.NettyObjects.getOrCreateEventLoopGroup("bossGroup", 1);
    static final EventLoopGroup LINKS = Shared.NettyObjects.getOrCreateEventLoopGroup("linksGroup", 8);
    static final EventLoopGroup STREAMS = Shared.NettyObjects.getOrCreateEventLoopGroup("streamsGroup", 8);

}
