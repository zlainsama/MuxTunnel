package me.lain.muxtun.sipo;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;

class RelayRequest extends DefaultPromise<RelayRequestResult>
{

    RelayRequest()
    {
        super(ImmediateEventExecutor.INSTANCE);
    }

    RelayRequest(EventExecutor executor)
    {
        super(executor);
    }

}
