package me.lain.muxtun.sipo;

import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Supplier;

public final class LinkConfig {

    private final Supplier<ProxyHandler> proxySupplier;
    private final short priority;
    private final long writeLimit;

    public LinkConfig(Supplier<ProxyHandler> proxySupplier, short priority, long writeLimit) {
        this.proxySupplier = Objects.requireNonNull(proxySupplier, "proxySupplier");
        this.priority = priority;
        this.writeLimit = writeLimit;
    }

    public Supplier<ProxyHandler> getProxySupplier() {
        return proxySupplier;
    }

    public short getPriority() {
        return priority;
    }

    public long getWriteLimit() {
        return writeLimit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinkConfig that = (LinkConfig) o;
        return priority == that.priority &&
                writeLimit == that.writeLimit &&
                proxySupplier.equals(that.proxySupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proxySupplier, priority, writeLimit);
    }

    @Override
    public String toString() {
        return "LinkConfig{" +
                "proxySupplier=" + proxySupplier +
                ", priority=" + priority +
                ", writeLimit=" + writeLimit +
                '}';
    }

    public static final class SimpleSocks5ProxyHandlerSupplier implements Supplier<ProxyHandler> {

        private final SocketAddress proxyAddress;

        public SimpleSocks5ProxyHandlerSupplier(SocketAddress proxyAddress) {
            this.proxyAddress = Objects.requireNonNull(proxyAddress, "proxyAddress");
        }

        @Override
        public ProxyHandler get() {
            return new Socks5ProxyHandler(proxyAddress);
        }

        public SocketAddress getProxyAddress() {
            return proxyAddress;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleSocks5ProxyHandlerSupplier that = (SimpleSocks5ProxyHandlerSupplier) o;
            return proxyAddress.equals(that.proxyAddress);
        }

        @Override
        public int hashCode() {
            return Objects.hash(proxyAddress);
        }

        @Override
        public String toString() {
            return "SimpleSocks5ProxyHandlerSupplier{" +
                    "proxyAddress=" + proxyAddress +
                    '}';
        }

    }

}
