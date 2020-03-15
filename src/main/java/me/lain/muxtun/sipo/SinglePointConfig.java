package me.lain.muxtun.sipo;

import java.net.SocketAddress;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;

public class SinglePointConfig
{

    private static final AtomicReference<Optional<GlobalTrafficShapingHandler>> globalTrafficShapingHandler = new AtomicReference<>(Optional.empty());

    public static final int DEFAULT_NUMLINKS = 4;
    public static final int DEFAULT_LIMITOPEN = 3;
    public static final int DEFAULT_MAXCLF = 8;

    public static Optional<GlobalTrafficShapingHandler> getGlobalTrafficShapingHandler()
    {
        return globalTrafficShapingHandler.get();
    }

    public static Optional<GlobalTrafficShapingHandler> setGlobalWriteReadLimit(long writeLimit, long readLimit)
    {
        if (writeLimit < 0L || readLimit < 0L)
            throw new IllegalArgumentException();

        if (writeLimit == 0L && readLimit == 0L)
            return globalTrafficShapingHandler.getAndSet(Optional.empty());
        else
            return globalTrafficShapingHandler.getAndSet(Optional.of(new GlobalTrafficShapingHandler(GlobalEventExecutor.INSTANCE, writeLimit, readLimit)));
    }

    final SocketAddress bindAddress;
    final SocketAddress remoteAddress;
    final Supplier<ProxyHandler> proxySupplier;
    final SslContext sslCtx;
    final UUID targetAddress;
    final byte[] secret;
    final byte[] secret_3;
    final int numLinks;
    final int limitOpen;
    final int maxCLF;
    final long writeLimit;
    final long readLimit;
    final String name;

    public SinglePointConfig(SocketAddress bindAddress, SocketAddress remoteAddress, Supplier<ProxyHandler> proxySupplier, SslContext sslCtx, UUID targetAddress, byte[] secret, byte[] secret_3)
    {
        this(bindAddress, remoteAddress, proxySupplier, sslCtx, targetAddress, secret, secret_3, DEFAULT_NUMLINKS, DEFAULT_LIMITOPEN, DEFAULT_MAXCLF, 0L, 0L);
    }

    public SinglePointConfig(SocketAddress bindAddress, SocketAddress remoteAddress, Supplier<ProxyHandler> proxySupplier, SslContext sslCtx, UUID targetAddress, byte[] secret, byte[] secret_3, int numLinks, int limitOpen, int maxCLF, long writeLimit, long readLimit)
    {
        this(bindAddress, remoteAddress, proxySupplier, sslCtx, targetAddress, secret, secret_3, numLinks, limitOpen, maxCLF, writeLimit, readLimit, "SinglePoint");
    }

    public SinglePointConfig(SocketAddress bindAddress, SocketAddress remoteAddress, Supplier<ProxyHandler> proxySupplier, SslContext sslCtx, UUID targetAddress, byte[] secret, byte[] secret_3, int numLinks, int limitOpen, int maxCLF, long writeLimit, long readLimit, String name)
    {
        if (bindAddress == null || remoteAddress == null || proxySupplier == null || sslCtx == null || targetAddress == null || (secret == null && secret_3 == null) || name == null)
            throw new NullPointerException();
        if (!sslCtx.isClient() || (secret != null && secret.length == 0) || (secret_3 != null && secret_3.length == 0) || numLinks < 1 || limitOpen < 1 || maxCLF < 0 || writeLimit < 0L || readLimit < 0L || name.isEmpty())
            throw new IllegalArgumentException();

        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
        this.proxySupplier = proxySupplier;
        this.sslCtx = sslCtx;
        this.targetAddress = targetAddress;
        this.secret = secret;
        this.secret_3 = secret_3;
        this.numLinks = numLinks;
        this.limitOpen = limitOpen;
        this.maxCLF = maxCLF;
        this.writeLimit = writeLimit;
        this.readLimit = readLimit;
        this.name = name;
    }

}
