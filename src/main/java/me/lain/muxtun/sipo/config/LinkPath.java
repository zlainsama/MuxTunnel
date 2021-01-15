package me.lain.muxtun.sipo.config;

import java.util.Objects;

public final class LinkPath {

    private Socks5ProxyHandlerSupplier proxySupplier;
    private short priority;
    private long writeLimit;

    public Socks5ProxyHandlerSupplier getProxySupplier() {
        return proxySupplier;
    }

    public LinkPath setProxySupplier(Socks5ProxyHandlerSupplier proxySupplier) {
        this.proxySupplier = proxySupplier;
        return this;
    }

    public short getPriority() {
        return priority;
    }

    public LinkPath setPriority(short priority) {
        this.priority = priority;
        return this;
    }

    public long getWriteLimit() {
        return writeLimit;
    }

    public LinkPath setWriteLimit(long writeLimit) {
        this.writeLimit = writeLimit;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinkPath linkPath = (LinkPath) o;
        return priority == linkPath.priority && writeLimit == linkPath.writeLimit && Objects.equals(proxySupplier, linkPath.proxySupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proxySupplier, priority, writeLimit);
    }

    @Override
    public String toString() {
        return "LinkPath{" +
                "proxySupplier=" + proxySupplier +
                ", priority=" + priority +
                ", writeLimit=" + writeLimit +
                '}';
    }

}
