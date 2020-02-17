package me.lain.muxtun;

import java.io.BufferedReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

public class App
{

    public static class SinglePointConfig
    {

        public final String name;

        public SocketAddress bindAddress = null;
        public SocketAddress remoteAddress = null;
        public Supplier<ProxyHandler> proxySupplier = null;
        public String trustSha1 = null;
        public UUID targetAddress = null;
        public int numLinks = SinglePoint.DEFAULT_NUMLINKS;
        public int limitOpen = SinglePoint.DEFAULT_LIMITOPEN;
        public SslContext sslCtx = null;

        public SinglePointConfig(String name)
        {
            this.name = name;
        }

    }

    private static Map<String, SinglePointConfig> configs = new HashMap<>();
    private static List<SinglePoint> points = new ArrayList<>();
    private static String profile = "SinglePoint";

    private static SinglePointConfig getConfig(String profile)
    {
        return configs.computeIfAbsent(profile, SinglePointConfig::new);
    }

    public static void main(String[] args) throws Exception
    {
        try (BufferedReader in = Files.newBufferedReader(FileSystems.getDefault().getPath(args.length > 0 ? args[0] : "MuxTunnel.cfg")))
        {
            System.out.println(String.format("%s > Loading config...", Shared.printNow()));
            in.lines().map(String::trim).filter(App::nonCommentLine).filter(App::validConfigLine).forEach(line -> {
                int i = line.indexOf("=");
                String name = line.substring(0, i).trim();
                String value = line.substring(i + 1).trim();
                if ("profile".equals(name))
                    profile = value;
                else if ("bindAddress".equals(name))
                {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    getConfig(profile).bindAddress = new InetSocketAddress(host, port);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "bindAddress", getConfig(profile).bindAddress.toString()));
                }
                else if ("remoteAddress".equals(name))
                {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    getConfig(profile).remoteAddress = new InetSocketAddress(host, port);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "remoteAddress", getConfig(profile).remoteAddress.toString()));
                }
                else if ("socks5Proxy".equals(name))
                {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    SocketAddress proxyAddress = new InetSocketAddress(host, port);
                    getConfig(profile).proxySupplier = () -> new Socks5ProxyHandler(proxyAddress);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "proxyAddress(socks5Proxy)", proxyAddress.toString()));
                }
                else if ("trustSha1".equals(name))
                {
                    getConfig(profile).trustSha1 = value;
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "trustSha1", getConfig(profile).trustSha1.toString()));
                }
                else if ("targetAddress".equals(name))
                {
                    getConfig(profile).targetAddress = UUID.fromString(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "targetAddress", getConfig(profile).targetAddress.toString()));
                }
                else if ("numLinks".equals(name))
                {
                    getConfig(profile).numLinks = Integer.parseInt(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "numLinks", getConfig(profile).numLinks));
                }
                else if ("limitOpen".equals(name))
                {
                    getConfig(profile).limitOpen = Integer.parseInt(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "limitOpen", getConfig(profile).limitOpen));
                }
            });
            System.out.println(String.format("%s > Done.", Shared.printNow()));

            System.out.println();
            System.out.println(String.format("%s > Checking config...", Shared.printNow()));
            boolean failed = false;
            for (SinglePointConfig config : configs.values())
            {
                if (config.bindAddress == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "bindAddress"));
                    failed = true;
                }
                if (config.remoteAddress == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "remoteAddress"));
                    failed = true;
                }
                if (config.proxySupplier == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "proxyAddress(socks5Proxy)"));
                    failed = true;
                }
                if (config.trustSha1 == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "trustSha1"));
                    failed = true;
                }
                if (config.targetAddress == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "targetAddress"));
                    failed = true;
                }
                if (config.numLinks < 1)
                {
                    System.out.println(String.format("%s > [%s] Invalid %s", Shared.printNow(), config.name, "numLinks"));
                    failed = true;
                }
                if (config.limitOpen < 1)
                {
                    System.out.println(String.format("%s > [%s] Invalid %s", Shared.printNow(), config.name, "limitOpen"));
                    failed = true;
                }
            }
            if (failed)
            {
                System.out.println(String.format("%s > Invalid config, Exit now.", Shared.printNow()));
                System.exit(1);
            }
            System.out.println(String.format("%s > Done.", Shared.printNow()));

            System.out.println();
            System.out.println(String.format("%s > Building SSLContext...", Shared.printNow()));
            for (SinglePointConfig config : configs.values())
                config.sslCtx = SslContextBuilder.forClient().trustManager(new FingerprintTrustManagerFactory(config.trustSha1)).build();
            System.out.println(String.format("%s > Done.", Shared.printNow()));
        }

        System.out.println();
        System.out.println(String.format("%s > Starting...", Shared.printNow()));
        for (SinglePointConfig config : configs.values())
            points.add(new SinglePoint(
                    config.bindAddress,
                    config.remoteAddress,
                    config.proxySupplier,
                    config.sslCtx,
                    config.targetAddress,
                    config.numLinks,
                    config.limitOpen,
                    config.name));
        System.out.println(String.format("%s > Done. %s", Shared.printNow(), points.toString()));
        System.out.println();
        Runtime.getRuntime().addShutdownHook(new Thread()
        {

            @Override
            public void run()
            {
                System.out.println();
                System.out.println(String.format("%s > Shutting down...", Shared.printNow()));
                Shared.bossGroup.shutdownGracefully().syncUninterruptibly();
                Shared.workerGroup.shutdownGracefully().syncUninterruptibly();
                for (SinglePoint point : points)
                    point.getChannels().close().syncUninterruptibly();
                System.out.println(String.format("%s > Done.", Shared.printNow()));
                System.out.println();
            }

        });
    }

    private static boolean nonCommentLine(String line)
    {
        return !line.startsWith("#");
    }

    private static boolean validConfigLine(String line)
    {
        return line.indexOf("=") != -1;
    }

}
