package me.lain.muxtun.sipo;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import me.lain.muxtun.Shared;
import me.lain.muxtun.util.SimpleLogger;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

class StreamBridge extends EmbeddedChannel implements Consumer<ByteBuf> {

    private final Queue<ByteBuf> buffer = new ConcurrentLinkedQueue<>();

    StreamBridge(Consumer<ByteBuf> outbound) {
        config().setAutoRead(false);
        closeFuture().addListener(future -> {
            ByteBuf pending;
            while ((pending = buffer.poll()) != null)
                ReferenceCountUtil.release(pending);
        });
        pipeline().addLast(new ChannelDuplexHandler() {

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                try {
                    if (msg instanceof ByteBuf) {
                        ByteBuf cast = (ByteBuf) msg;

                        handleMessage(StreamContext.getContext(ctx.channel()), cast);
                    }
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                ctx.close().addListener(future -> SimpleLogger.println("%s > stream bridge %s closed with unexpected error. (%s)", Shared.printNow(), ctx.channel(), cause));
            }

            private void handleMessage(StreamContext sctx, ByteBuf msg) throws Exception {
                if (sctx.isActive() && !sctx.getPayloadWriter().writeSlices(msg.retain()))
                    sctx.close();
            }

            @Override
            public void read(ChannelHandlerContext ctx) throws Exception {
                Optional.ofNullable(buffer.poll()).ifPresent(StreamBridge.this::writeInbound);
            }

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                try {
                    if (promise.setUncancellable() && !promise.isDone()) {
                        if (msg instanceof ByteBuf) {
                            ByteBuf cast = (ByteBuf) msg;

                            outbound.accept(cast.retain());
                            if (!promise.isVoid())
                                promise.trySuccess();
                        }
                    }
                } catch (Throwable e) {
                    if (!promise.isVoid())
                        promise.tryFailure(e);
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }

        });
    }

    @Override
    public void accept(ByteBuf t) {
        try {
            if (isActive()) {
                buffer.add(t.retain());

                if (config().isAutoRead())
                    read();
            }
        } finally {
            ReferenceCountUtil.release(t);
        }
    }

}
