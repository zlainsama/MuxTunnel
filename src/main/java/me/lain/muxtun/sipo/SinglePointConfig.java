package me.lain.muxtun.sipo;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import me.lain.muxtun.Shared;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;

public class SinglePointConfig {

    public static final int DEFAULT_NUMSESSIONS = 1;
    public static final int DEFAULT_MAXCLF = 8;

    private final SocketAddress bindAddress;
    private final SocketAddress remoteAddress;
    private final LinkConfig[] linkConfigs;
    private final SslContext sslCtx;
    private final UUID targetAddress;
    private final int numSessions;
    private final int maxCLF;
    private final String name;

    public SinglePointConfig(SocketAddress bindAddress, SocketAddress remoteAddress, LinkConfig[] linkConfigs, SslContext sslCtx, UUID targetAddress) {
        this(bindAddress, remoteAddress, linkConfigs, sslCtx, targetAddress, DEFAULT_NUMSESSIONS, DEFAULT_MAXCLF);
    }

    public SinglePointConfig(SocketAddress bindAddress, SocketAddress remoteAddress, LinkConfig[] linkConfigs, SslContext sslCtx, UUID targetAddress, int numSessions, int maxCLF) {
        this(bindAddress, remoteAddress, linkConfigs, sslCtx, targetAddress, numSessions, maxCLF, "SinglePoint");
    }

    public SinglePointConfig(SocketAddress bindAddress, SocketAddress remoteAddress, LinkConfig[] linkConfigs, SslContext sslCtx, UUID targetAddress, int numSessions, int maxCLF, String name) {
        if (bindAddress == null || remoteAddress == null || linkConfigs == null || sslCtx == null || targetAddress == null || name == null)
            throw new NullPointerException();
        if (linkConfigs.length == 0 || !sslCtx.isClient() || numSessions < 1 || maxCLF < 0 || name.isEmpty())
            throw new IllegalArgumentException();

        this.bindAddress = bindAddress;
        this.remoteAddress = remoteAddress;
        this.linkConfigs = linkConfigs;
        this.sslCtx = sslCtx;
        this.targetAddress = targetAddress;
        this.numSessions = numSessions;
        this.maxCLF = maxCLF;
        this.name = name;
    }

    public static SslContext buildContext(Path pathCert, Path pathKey, List<String> trustSha256, List<String> ciphers, List<String> protocols) throws IOException {
        return SslContextBuilder.forClient().keyManager(Files.newInputStream(pathCert, StandardOpenOption.READ), Files.newInputStream(pathKey, StandardOpenOption.READ))
                .clientAuth(ClientAuth.REQUIRE)
                .trustManager(FingerprintTrustManagerFactory.builder("SHA-256").fingerprints(trustSha256).build())
                .ciphers(!ciphers.isEmpty() ? ciphers : !Shared.TLS.defaultCipherSuites.isEmpty() ? Shared.TLS.defaultCipherSuites : null, SupportedCipherSuiteFilter.INSTANCE)
                .protocols(!protocols.isEmpty() ? protocols : !Shared.TLS.defaultProtocols.isEmpty() ? Shared.TLS.defaultProtocols : null)
                .build();
    }

    public SocketAddress getBindAddress() {
        return bindAddress;
    }

    public int getMaxCLF() {
        return maxCLF;
    }

    public String getName() {
        return name;
    }

    public int getNumSessions() {
        return numSessions;
    }

    public LinkConfig[] getLinkConfigs() {
        return linkConfigs;
    }

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public SslContext getSslCtx() {
        return sslCtx;
    }

    public UUID getTargetAddress() {
        return targetAddress;
    }

}
