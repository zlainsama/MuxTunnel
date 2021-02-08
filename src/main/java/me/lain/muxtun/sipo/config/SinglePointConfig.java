package me.lain.muxtun.sipo.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import me.lain.muxtun.Shared;
import me.lain.muxtun.sipo.config.adapter.LinkPathAdapter;
import me.lain.muxtun.sipo.config.adapter.PathAdapter;
import me.lain.muxtun.sipo.config.adapter.SocketAddressAdapter;
import me.lain.muxtun.sipo.config.adapter.UUIDAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public final class SinglePointConfig {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(SocketAddress.class, new SocketAddressAdapter())
            .registerTypeAdapter(UUID.class, new UUIDAdapter())
            .registerTypeAdapter(Path.class, new PathAdapter())
            .registerTypeAdapter(LinkPath.class, new LinkPathAdapter())
            .create();

    private SocketAddress bindAddress;
    private SocketAddress remoteAddress;
    private List<LinkPath> linkPaths;
    private Path pathCert;
    private Path pathKey;
    private List<String> trusts;
    private List<String> ciphers;
    private List<String> protocols;
    private UUID targetAddress;
    private int numSessions;
    private int maxCLF;

    public static SslContext buildContext(Path pathCert, Path pathKey, List<String> trustSha256s, List<String> ciphers, List<String> protocols) throws IOException {
        return buildContext(pathCert, pathKey, trustSha256s, "SHA-256", ciphers, protocols);
    }

    public static SslContext buildContext(Path pathCert, Path pathKey, List<String> trusts, String algorithm, List<String> ciphers, List<String> protocols) throws IOException {
        try (InputStream inCert = Files.newInputStream(pathCert, StandardOpenOption.READ); InputStream inKey = Files.newInputStream(pathKey, StandardOpenOption.READ)) {
            return SslContextBuilder.forClient().keyManager(inCert, inKey)
                    .clientAuth(ClientAuth.REQUIRE)
                    .trustManager(FingerprintTrustManagerFactory.builder(algorithm).fingerprints(trusts).build())
                    .ciphers(!ciphers.isEmpty() ? ciphers : !Shared.TLS.defaultCipherSuites.isEmpty() ? Shared.TLS.defaultCipherSuites : null, SupportedCipherSuiteFilter.INSTANCE)
                    .protocols(!protocols.isEmpty() ? protocols : !Shared.TLS.defaultProtocols.isEmpty() ? Shared.TLS.defaultProtocols : null)
                    .build();
        }
    }

    public static SinglePointConfig fromJson(String json) {
        return gson.fromJson(json, SinglePointConfig.class);
    }

    public static List<SinglePointConfig> fromJsonList(String json) {
        return gson.fromJson(json, new TypeToken<ArrayList<SinglePointConfig>>() {
        }.getType());
    }

    public static Map<String, SinglePointConfig> fromJsonMap(String json) {
        return gson.fromJson(json, new TypeToken<HashMap<String, SinglePointConfig>>() {
        }.getType());
    }

    public static String toJson(SinglePointConfig config) {
        return gson.toJson(config);
    }

    public static String toJsonList(List<SinglePointConfig> configList) {
        return gson.toJson(configList);
    }

    public static String toJsonMap(Map<String, SinglePointConfig> configMap) {
        return gson.toJson(configMap);
    }

    public SocketAddress getBindAddress() {
        return bindAddress;
    }

    public SinglePointConfig setBindAddress(SocketAddress bindAddress) {
        this.bindAddress = bindAddress;
        return this;
    }

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public SinglePointConfig setRemoteAddress(SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
        return this;
    }

    public List<LinkPath> getLinkPaths() {
        return linkPaths;
    }

    public SinglePointConfig setLinkPaths(List<LinkPath> linkPaths) {
        this.linkPaths = linkPaths;
        return this;
    }

    public Path getPathCert() {
        return pathCert;
    }

    public SinglePointConfig setPathCert(Path pathCert) {
        this.pathCert = pathCert;
        return this;
    }

    public Path getPathKey() {
        return pathKey;
    }

    public SinglePointConfig setPathKey(Path pathKey) {
        this.pathKey = pathKey;
        return this;
    }

    public List<String> getTrusts() {
        return trusts;
    }

    public SinglePointConfig setTrusts(List<String> trusts) {
        this.trusts = trusts;
        return this;
    }

    public List<String> getCiphers() {
        return ciphers;
    }

    public SinglePointConfig setCiphers(List<String> ciphers) {
        this.ciphers = ciphers;
        return this;
    }

    public List<String> getProtocols() {
        return protocols;
    }

    public SinglePointConfig setProtocols(List<String> protocols) {
        this.protocols = protocols;
        return this;
    }

    public UUID getTargetAddress() {
        return targetAddress;
    }

    public SinglePointConfig setTargetAddress(UUID targetAddress) {
        this.targetAddress = targetAddress;
        return this;
    }

    public int getNumSessions() {
        return numSessions;
    }

    public SinglePointConfig setNumSessions(int numSessions) {
        this.numSessions = numSessions;
        return this;
    }

    public int getMaxCLF() {
        return maxCLF;
    }

    public SinglePointConfig setMaxCLF(int maxCLF) {
        this.maxCLF = maxCLF;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SinglePointConfig that = (SinglePointConfig) o;
        return numSessions == that.numSessions && maxCLF == that.maxCLF && Objects.equals(bindAddress, that.bindAddress) && Objects.equals(remoteAddress, that.remoteAddress) && Objects.equals(linkPaths, that.linkPaths) && Objects.equals(pathCert, that.pathCert) && Objects.equals(pathKey, that.pathKey) && Objects.equals(trusts, that.trusts) && Objects.equals(ciphers, that.ciphers) && Objects.equals(protocols, that.protocols) && Objects.equals(targetAddress, that.targetAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bindAddress, remoteAddress, linkPaths, pathCert, pathKey, trusts, ciphers, protocols, targetAddress, numSessions, maxCLF);
    }

    @Override
    public String toString() {
        return "SinglePointConfig{" +
                "bindAddress=" + bindAddress +
                ", remoteAddress=" + remoteAddress +
                ", linkPaths=" + linkPaths +
                ", pathCert=" + pathCert +
                ", pathKey=" + pathKey +
                ", trusts=" + trusts +
                ", ciphers=" + ciphers +
                ", protocols=" + protocols +
                ", targetAddress=" + targetAddress +
                ", numSessions=" + numSessions +
                ", maxCLF=" + maxCLF +
                '}';
    }

}
