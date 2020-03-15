package me.lain.muxtun.sipo;

import java.util.UUID;

class LinkSessionEvent
{

    final LinkSessionEventType type;
    final UUID streamId;
    final boolean clfLimitExceeded;

    LinkSessionEvent(LinkSessionEventType type, boolean clfLimitExceeded)
    {
        this.type = type;
        this.streamId = null;
        this.clfLimitExceeded = clfLimitExceeded;
    }

    LinkSessionEvent(LinkSessionEventType type, UUID streamId, boolean clfLimitExceeded)
    {
        this.type = type;
        this.streamId = streamId;
        this.clfLimitExceeded = clfLimitExceeded;
    }

}
