package me.lain.muxtun.sipo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutorGroup;
import me.lain.muxtun.Shared;
import me.lain.muxtun.codec.Message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Vars {

    static final String HANDLERNAME_TLS = "TLS";
    static final String HANDLERNAME_CODEC = "Codec";
    static final String HANDLERNAME_HANDLER = "Handler";
    static final String HANDLERNAME_LIMITER = "Limiter";

    static final AttributeKey<LinkContext> LINKCONTEXT_KEY = AttributeKey.valueOf("me.lain.muxtun.sipo.Vars#LinkContext");
    static final AttributeKey<StreamContext> STREAMCONTEXT_KEY = AttributeKey.valueOf("me.lain.muxtun.sipo.Vars#StreamContext");

    static final int GROUP_THREADS = Math.max(2, Math.min(Runtime.getRuntime().availableProcessors() * 2, Short.MAX_VALUE));
    static final EventLoopGroup WORKERS = Shared.NettyObjects.getOrCreateEventLoopGroup("workersGroup", GROUP_THREADS);
    static final EventExecutorGroup SESSIONS = Shared.NettyObjects.getOrCreateEventExecutorGroup("sessionsGroup", GROUP_THREADS);
    static final ExecutorService SHARED_POOL = Executors.newWorkStealingPool(GROUP_THREADS);

    static final Message PLACEHOLDER = new Message() {

        @Override
        public Message copy() {
            throw new UnsupportedOperationException("PLACEHOLDER");
        }

        @Override
        public void decode(ByteBuf buf) throws Exception {
            throw new UnsupportedOperationException("PLACEHOLDER");
        }

        @Override
        public void encode(ByteBuf buf) throws Exception {
            throw new UnsupportedOperationException("PLACEHOLDER");
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException("PLACEHOLDER");
        }

        @Override
        public MessageType type() {
            throw new UnsupportedOperationException("PLACEHOLDER");
        }

    };

}
