package me.lain.muxtun.sipo;

import java.util.UUID;
import io.netty.channel.Channel;

class RelayRequestResult
{

    final Channel linkChannel;
    final LinkSession session;
    final UUID streamId;

    RelayRequestResult(Channel linkChannel, LinkSession session, UUID streamId)
    {
        this.linkChannel = linkChannel;
        this.session = session;
        this.streamId = streamId;
    }

}
