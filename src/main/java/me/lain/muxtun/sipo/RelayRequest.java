package me.lain.muxtun.sipo;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;

class RelayRequest extends DefaultPromise<RelayRequestResult> {

    RelayRequest(EventExecutor executor) {
        super(executor);
    }

}
