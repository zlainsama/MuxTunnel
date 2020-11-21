package me.lain.muxtun;

import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import me.lain.muxtun.sipo.LinkConfig;
import me.lain.muxtun.sipo.SinglePoint;
import me.lain.muxtun.sipo.SinglePointConfig;
import me.lain.muxtun.util.SimpleLogger;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class App {

    private static final List<SinglePoint> points = new ArrayList<>();

    private static Map<String, SinglePointTempConfig> configs = new HashMap<>();
    private static String profile = "SinglePoint";

    private static void discardOut() {
        System.setOut(new PrintStream(Shared.voidStream));
        System.setErr(new PrintStream(Shared.voidStream));
    }

    private static SinglePointTempConfig getConfig(String profile) {
        return configs.computeIfAbsent(profile, SinglePointTempConfig::new);
    }

    private static Path init(String... args) throws Exception {
        int index = 0;
        boolean nolog = false;
        boolean silent = false;
        Optional<Path> pathLog = Optional.empty();
        Optional<Path> pathConfig = Optional.empty();

        for (String arg : args) {
            if (arg.startsWith("-")) {
                if ("--nolog".equalsIgnoreCase(arg))
                    nolog = true;
                else if ("--silent".equalsIgnoreCase(arg))
                    silent = true;
            } else {
                switch (index++) {
                    case 0:
                        pathConfig = Optional.of(FileSystems.getDefault().getPath(arg));
                        break;
                    case 1:
                        pathLog = Optional.of(FileSystems.getDefault().getPath(arg));
                        break;
                }
            }
        }

        if (silent)
            discardOut();
        if (!nolog)
            SimpleLogger.setFileOut(pathLog.orElse(FileSystems.getDefault().getPath("MuxTunnel.log")));

        return pathConfig.orElse(FileSystems.getDefault().getPath("MuxTunnel.cfg"));
    }

    public static void main(String[] args) throws Exception {
        try (BufferedReader in = Files.newBufferedReader(init(args))) {
            in.lines().map(String::trim).filter(App::nonCommentLine).filter(App::validConfigLine).forEach(line -> {
                int i = line.indexOf("=");
                String name = line.substring(0, i).trim();
                String value = line.substring(i + 1).trim();
                if ("profile".equals(name)) {
                    if ("Global".equals(value))
                        SimpleLogger.println("%s > Ignored \"profile = %s\", \"%s\" is a reserved name.", Shared.printNow(), value, value);
                    else
                        profile = value;
                } else if ("bindAddress".equals(name)) {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    getConfig(profile).bindAddress = new InetSocketAddress(host, port);
                } else if ("remoteAddress".equals(name)) {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    getConfig(profile).remoteAddress = new InetSocketAddress(host, port);
                } else if ("linkPath".equals(name)) {
                    int i1 = value.indexOf("@");
                    String configString = value.substring(0, i1);
                    String addressString = value.substring(i1 + 1);
                    int i2 = configString.indexOf(":");
                    short priority = Short.parseShort(configString.substring(0, i2));
                    long writeLimit = Long.parseLong(configString.substring(i2 + 1));
                    int i3 = addressString.lastIndexOf(":");
                    String host = addressString.substring(0, i3);
                    int port = Integer.parseInt(addressString.substring(i3 + 1));
                    SocketAddress proxyAddress = new InetSocketAddress(host, port);
                    Supplier<ProxyHandler> proxySupplier = new LinkConfig.SimpleSocks5ProxyHandlerSupplier(proxyAddress);
                    LinkConfig linkConfig = new LinkConfig(proxySupplier, priority, writeLimit);
                    getConfig(profile).linkConfigs.add(linkConfig);
                } else if ("pathCert".equals(name)) {
                    getConfig(profile).pathCert = FileSystems.getDefault().getPath(value);
                } else if ("pathKey".equals(name)) {
                    getConfig(profile).pathKey = FileSystems.getDefault().getPath(value);
                } else if ("trustSha256".equals(name)) {
                    getConfig(profile).trustSha256.add(value);
                } else if ("ciphers".equals(name)) {
                    getConfig(profile).ciphers.addAll(Arrays.asList(value.split(":")));
                } else if ("protocols".equals(name)) {
                    getConfig(profile).protocols.addAll(Arrays.asList(value.split(":")));
                } else if ("targetAddress".equals(name)) {
                    getConfig(profile).targetAddress = UUID.fromString(value);
                } else if ("numSessions".equals(name)) {
                    getConfig(profile).numSessions = Integer.parseInt(value);
                } else if ("maxCLF".equals(name)) {
                    getConfig(profile).maxCLF = Integer.parseInt(value);
                }
            });

            boolean failed = false;
            for (SinglePointTempConfig config : configs.values()) {
                if (config.bindAddress == null) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "bindAddress");
                    failed = true;
                }
                if (config.remoteAddress == null) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "remoteAddress");
                    failed = true;
                }
                if (config.linkConfigs.isEmpty()) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "linkPath");
                    failed = true;
                }
                if (config.pathCert == null) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "pathCert");
                    failed = true;
                }
                if (config.pathKey == null) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "pathKey");
                    failed = true;
                }
                if (config.trustSha256.isEmpty()) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "trustSha256");
                    failed = true;
                }
                if (config.targetAddress == null) {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "targetAddress");
                    failed = true;
                }
                if (config.numSessions < 1) {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "numSessions");
                    failed = true;
                }
                if (config.maxCLF < 0) {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "maxCLF");
                    failed = true;
                }
            }
            if (failed) {
                SimpleLogger.println("%s > Invalid config, Exit now.", Shared.printNow());
                System.exit(1);
            }

            for (SinglePointTempConfig config : configs.values())
                config.sslCtx = SinglePointConfig.buildContext(config.pathCert, config.pathKey, config.trustSha256, config.ciphers, config.protocols);
        }

        SimpleLogger.println("%s > Starting...", Shared.printNow());
        while (true) {
            points.addAll(configs.values().stream().map(SinglePointTempConfig::finish).map(SinglePoint::new).collect(Collectors.toList()));
            if (Shared.combineFutures(points.stream().map(SinglePoint::start).collect(Collectors.toList())).awaitUninterruptibly(60L, TimeUnit.SECONDS))
                break;
            Shared.combineFutures(points.stream().map(SinglePoint::stop).collect(Collectors.toList())).awaitUninterruptibly(60L, TimeUnit.SECONDS);
            points.clear();
            Shared.sleep(5000L);
        }
        SimpleLogger.println("%s > Done. %s", Shared.printNow(), points.toString());

        configs = null;
        profile = null;

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                SimpleLogger.println("%s > Shutting down...", Shared.printNow());
                List<Future<?>> futures = new ArrayList<>();
                futures.addAll(Shared.NettyObjects.shutdownGracefully());
                futures.addAll(points.stream().map(SinglePoint::stop).collect(Collectors.toList()));
                Shared.combineFutures(futures).awaitUninterruptibly(60L, TimeUnit.SECONDS);
                SimpleLogger.println("%s > Done.", Shared.printNow());
                Shared.sleep(100L);
            }

        });
    }

    private static boolean nonCommentLine(String line) {
        return !line.startsWith("#");
    }

    private static boolean validConfigLine(String line) {
        return line.indexOf("=") != -1;
    }

    public static class SinglePointTempConfig {

        public final String name;

        public SocketAddress bindAddress = null;
        public SocketAddress remoteAddress = null;
        public List<LinkConfig> linkConfigs = new ArrayList<>();
        public Path pathCert = null;
        public Path pathKey = null;
        public List<String> trustSha256 = new ArrayList<>();
        public List<String> ciphers = new ArrayList<>();
        public List<String> protocols = new ArrayList<>();
        public UUID targetAddress = null;
        public int numSessions = SinglePointConfig.DEFAULT_NUMSESSIONS;
        public int maxCLF = SinglePointConfig.DEFAULT_MAXCLF;
        public SslContext sslCtx = null;

        public SinglePointTempConfig(String name) {
            this.name = name;
        }

        public SinglePointConfig finish() {
            return new SinglePointConfig(
                    bindAddress,
                    remoteAddress,
                    linkConfigs.toArray(new LinkConfig[0]),
                    sslCtx,
                    targetAddress,
                    numSessions,
                    maxCLF,
                    name);
        }

    }

}
