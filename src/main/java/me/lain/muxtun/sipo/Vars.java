package me.lain.muxtun.sipo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.util.AttributeKey;
import me.lain.muxtun.Shared;
import me.lain.muxtun.codec.Message;

class Vars {

    static final AttributeKey<LinkContext> LINKCONTEXT_KEY = AttributeKey.valueOf("me.lain.muxtun.sipo.Vars#LinkContext");
    static final AttributeKey<StreamContext> STREAMCONTEXT_KEY = AttributeKey.valueOf("me.lain.muxtun.sipo.Vars#StreamContext");

    static final EventLoopGroup WORKERS = Shared.NettyObjects.getOrCreateEventLoopGroup("workersGroup", Math.max(4, Math.min(Runtime.getRuntime().availableProcessors() * 2, Short.MAX_VALUE)));

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
