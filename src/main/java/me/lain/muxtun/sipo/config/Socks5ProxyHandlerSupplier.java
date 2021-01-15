package me.lain.muxtun.sipo.config;

import io.netty.handler.proxy.Socks5ProxyHandler;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Supplier;

public final class Socks5ProxyHandlerSupplier implements Supplier<Socks5ProxyHandler> {

    private SocketAddress proxyAddress;
    private String username;
    private String password;

    public SocketAddress getProxyAddress() {
        return proxyAddress;
    }

    public Socks5ProxyHandlerSupplier setProxyAddress(SocketAddress proxyAddress) {
        this.proxyAddress = proxyAddress;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public Socks5ProxyHandlerSupplier setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Socks5ProxyHandlerSupplier setPassword(String password) {
        this.password = password;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Socks5ProxyHandlerSupplier that = (Socks5ProxyHandlerSupplier) o;
        return Objects.equals(proxyAddress, that.proxyAddress) && Objects.equals(username, that.username) && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proxyAddress, username, password);
    }

    @Override
    public String toString() {
        return "Socks5ProxyHandlerSupplier{" +
                "proxyAddress=" + proxyAddress +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    @Override
    public Socks5ProxyHandler get() {
        return new Socks5ProxyHandler(proxyAddress, username, password);
    }

}
