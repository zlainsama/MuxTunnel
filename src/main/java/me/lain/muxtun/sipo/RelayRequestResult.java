package me.lain.muxtun.sipo;

import java.util.UUID;

class RelayRequestResult
{

    private final LinkSession session;
    private final UUID streamId;

    RelayRequestResult(LinkSession session, UUID streamId)
    {
        this.session = session;
        this.streamId = streamId;
    }

    LinkSession getSession()
    {
        return session;
    }

    UUID getStreamId()
    {
        return streamId;
    }

}
