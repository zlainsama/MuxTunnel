package me.lain.muxtun;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import io.netty.util.concurrent.Future;
import me.lain.muxtun.sipo.SinglePoint;
import me.lain.muxtun.sipo.SinglePointConfig;
import me.lain.muxtun.util.SimpleLogger;

public class App
{

    public static class SinglePointTempConfig
    {

        public final String name;

        public SocketAddress bindAddress = null;
        public SocketAddress remoteAddress = null;
        public Supplier<ProxyHandler> proxySupplier = null;
        public List<String> trustSha1 = new ArrayList<>();
        public List<String> ciphers = new ArrayList<>();
        public List<String> protocols = new ArrayList<>();
        public UUID targetAddress = null;
        public Path pathSecret = null;
        public Path pathSecret_3 = null;
        public int numLinks = SinglePointConfig.DEFAULT_NUMLINKS;
        public int limitOpen = SinglePointConfig.DEFAULT_LIMITOPEN;
        public int maxCLF = SinglePointConfig.DEFAULT_MAXCLF;
        public long writeLimit = 0L;
        public long readLimit = 0L;
        public SslContext sslCtx = null;
        public byte[] secret = null;
        public byte[] secret_3 = null;

        public SinglePointTempConfig(String name)
        {
            this.name = name;
        }

        public SinglePointConfig finish()
        {
            return new SinglePointConfig(
                    bindAddress,
                    remoteAddress,
                    proxySupplier,
                    sslCtx,
                    targetAddress,
                    secret,
                    secret_3,
                    numLinks,
                    limitOpen,
                    maxCLF,
                    writeLimit,
                    readLimit,
                    name);
        }

    }

    private static Map<String, SinglePointTempConfig> configs = new HashMap<>();
    private static List<SinglePoint> points = new ArrayList<>();
    private static String profile = "SinglePoint";
    private static long globalWriteLimit = 0L;
    private static long globalReadLimit = 0L;

    private static void discardOut()
    {
        System.setOut(new PrintStream(Shared.voidStream));
        System.setErr(new PrintStream(Shared.voidStream));
    }

    private static byte[] generateSecret(Path path, byte[] magic)
    {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ))
        {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            fc.transferTo(0L, Long.MAX_VALUE, Channels.newChannel(new DigestOutputStream(Shared.voidStream, md)));

            return md.digest(magic);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    private static byte[] generateSecret_3(Path path, byte[] magic)
    {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ))
        {
            MessageDigest md = MessageDigest.getInstance("SHA3-256");

            fc.transferTo(0L, Long.MAX_VALUE, Channels.newChannel(new DigestOutputStream(Shared.voidStream, md)));

            return md.digest(magic);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    private static SinglePointTempConfig getConfig(String profile)
    {
        return configs.computeIfAbsent(profile, SinglePointTempConfig::new);
    }

    private static Path init(String... args) throws Exception
    {
        int index = 0;
        boolean nolog = false;
        boolean silent = false;
        Optional<Path> pathLog = Optional.empty();
        Optional<Path> pathConfig = Optional.empty();

        for (String arg : args)
        {
            if (arg.startsWith("-"))
            {
                if ("--nolog".equalsIgnoreCase(arg))
                    nolog = true;
                else if ("--silent".equalsIgnoreCase(arg))
                    silent = true;
            }
            else
            {
                switch (index++)
                {
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

    public static void main(String[] args) throws Exception
    {
        try (BufferedReader in = Files.newBufferedReader(init(args)))
        {
            SimpleLogger.println("%s > Loading config...", Shared.printNow());
            in.lines().map(String::trim).filter(App::nonCommentLine).filter(App::validConfigLine).forEach(line -> {
                int i = line.indexOf("=");
                String name = line.substring(0, i).trim();
                String value = line.substring(i + 1).trim();
                if ("profile".equals(name))
                {
                    if ("Global".equals(value))
                        SimpleLogger.println("%s > Ignored \"profile = %s\", \"%s\" is a reserved name.", Shared.printNow(), value, value);
                    else
                        profile = value;
                }
                else if ("bindAddress".equals(name))
                {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    getConfig(profile).bindAddress = new InetSocketAddress(host, port);
                    SimpleLogger.println("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "bindAddress", getConfig(profile).bindAddress.toString());
                }
                else if ("remoteAddress".equals(name))
                {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    getConfig(profile).remoteAddress = new InetSocketAddress(host, port);
                    SimpleLogger.println("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "remoteAddress", getConfig(profile).remoteAddress.toString());
                }
                else if ("socks5Proxy".equals(name))
                {
                    int i1 = value.lastIndexOf(":");
                    String host = value.substring(0, i1);
                    int port = Integer.parseInt(value.substring(i1 + 1));
                    SocketAddress proxyAddress = new InetSocketAddress(host, port);
                    getConfig(profile).proxySupplier = () -> new Socks5ProxyHandler(proxyAddress);
                    SimpleLogger.println("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "proxyAddress(socks5Proxy)", proxyAddress.toString());
                }
                else if ("trustSha1".equals(name))
                {
                    getConfig(profile).trustSha1.add(value);
                    SimpleLogger.println("%s > [%s] %s has been added to %s", Shared.printNow(), profile, value, "trustSha1");
                }
                else if ("ciphers".equals(name))
                {
                    List<String> toAdd = Arrays.asList(value.split(":"));
                    getConfig(profile).ciphers.addAll(toAdd);
                    for (String c : toAdd)
                        SimpleLogger.println("%s > [%s] %s has been added to %s", Shared.printNow(), profile, c, "ciphers");
                }
                else if ("protocols".equals(name))
                {
                    List<String> toAdd = Arrays.asList(value.split(":"));
                    getConfig(profile).protocols.addAll(toAdd);
                    for (String c : toAdd)
                        SimpleLogger.println("%s > [%s] %s has been added to %s", Shared.printNow(), profile, c, "protocols");
                }
                else if ("targetAddress".equals(name))
                {
                    getConfig(profile).targetAddress = UUID.fromString(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "targetAddress", getConfig(profile).targetAddress.toString());
                }
                else if ("pathSecret".equals(name))
                {
                    getConfig(profile).pathSecret = FileSystems.getDefault().getPath(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "pathSecret", getConfig(profile).pathSecret.toString());
                }
                else if ("pathSecret_3".equals(name))
                {
                    getConfig(profile).pathSecret_3 = FileSystems.getDefault().getPath(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "pathSecret_3", getConfig(profile).pathSecret_3.toString());
                }
                else if ("numLinks".equals(name))
                {
                    getConfig(profile).numLinks = Integer.parseInt(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "numLinks", getConfig(profile).numLinks);
                }
                else if ("limitOpen".equals(name))
                {
                    getConfig(profile).limitOpen = Integer.parseInt(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "limitOpen", getConfig(profile).limitOpen);
                }
                else if ("maxCLF".equals(name))
                {
                    getConfig(profile).maxCLF = Integer.parseInt(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "maxCLF", getConfig(profile).maxCLF);
                }
                else if ("writeLimit".equals(name))
                {
                    getConfig(profile).writeLimit = Long.parseLong(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "writeLimit", getConfig(profile).writeLimit);
                }
                else if ("readLimit".equals(name))
                {
                    getConfig(profile).readLimit = Long.parseLong(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "readLimit", getConfig(profile).readLimit);
                }
                else if ("globalWriteLimit".equals(name))
                {
                    globalWriteLimit = Long.parseLong(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), "Global", "globalWriteLimit", globalWriteLimit);
                }
                else if ("globalReadLimit".equals(name))
                {
                    globalReadLimit = Long.parseLong(value);
                    SimpleLogger.println("%s > [%s] %s has been set to %d", Shared.printNow(), "Global", "globalReadLimit", globalReadLimit);
                }
            });
            SimpleLogger.println("%s > Done.", Shared.printNow());

            SimpleLogger.println();
            SimpleLogger.println("%s > Checking config...", Shared.printNow());
            boolean failed = false;
            for (SinglePointTempConfig config : configs.values())
            {
                if (config.bindAddress == null)
                {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "bindAddress");
                    failed = true;
                }
                if (config.remoteAddress == null)
                {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "remoteAddress");
                    failed = true;
                }
                if (config.proxySupplier == null)
                {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "proxyAddress(socks5Proxy)");
                    failed = true;
                }
                if (config.trustSha1.isEmpty())
                {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "trustSha1");
                    failed = true;
                }
                if (config.targetAddress == null)
                {
                    SimpleLogger.println("%s > [%s] Missing %s", Shared.printNow(), config.name, "targetAddress");
                    failed = true;
                }
                if (config.pathSecret == null && config.pathSecret_3 == null)
                {
                    SimpleLogger.println("%s > [%s] Missing %s and %s", Shared.printNow(), config.name, "pathSecret", "pathSecret_3");
                    failed = true;
                }
                if (config.numLinks < 1)
                {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "numLinks");
                    failed = true;
                }
                if (config.limitOpen < 1)
                {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "limitOpen");
                    failed = true;
                }
                if (config.maxCLF < 0)
                {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "maxCLF");
                    failed = true;
                }
                if (config.writeLimit < 0L)
                {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "writeLimit");
                    failed = true;
                }
                if (config.readLimit < 0L)
                {
                    SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), config.name, "readLimit");
                    failed = true;
                }
            }
            if (globalWriteLimit < 0L)
            {
                SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), "Global", "globalWriteLimit");
                failed = true;
            }
            if (globalReadLimit < 0L)
            {
                SimpleLogger.println("%s > [%s] Invalid %s", Shared.printNow(), "Global", "globalReadLimit");
                failed = true;
            }
            if (failed)
            {
                SimpleLogger.println("%s > Invalid config, Exit now.", Shared.printNow());
                System.exit(1);
            }
            SinglePointConfig.setGlobalWriteReadLimit(globalWriteLimit, globalReadLimit);
            SimpleLogger.println("%s > Done.", Shared.printNow());

            SimpleLogger.println();
            SimpleLogger.println("%s > Building SSLContext...", Shared.printNow());
            for (SinglePointTempConfig config : configs.values())
                config.sslCtx = SslContextBuilder.forClient().trustManager(new FingerprintTrustManagerFactory(config.trustSha1)).ciphers(!config.ciphers.isEmpty() ? config.ciphers : Shared.TLS.defaultCipherSuites, SupportedCipherSuiteFilter.INSTANCE).protocols(!config.protocols.isEmpty() ? config.protocols : Shared.TLS.defaultProtocols).build();
            SimpleLogger.println("%s > Done.", Shared.printNow());

            SimpleLogger.println();
            SimpleLogger.println("%s > Generating Secret...", Shared.printNow());
            for (SinglePointTempConfig config : configs.values())
            {
                config.secret = generateSecret(config.pathSecret, Shared.magic);
                config.secret_3 = generateSecret_3(config.pathSecret_3 != null ? config.pathSecret_3 : config.pathSecret, Shared.magic);
            }
            SimpleLogger.println("%s > Done.", Shared.printNow());
        }

        SimpleLogger.println();
        SimpleLogger.println("%s > Starting...", Shared.printNow());
        points.addAll(configs.values().stream().map(SinglePointTempConfig::finish).map(SinglePoint::new).collect(Collectors.toList()));
        points.stream().map(SinglePoint::start).collect(Collectors.toList()).forEach(Future::syncUninterruptibly);
        SimpleLogger.println("%s > Done. %s", Shared.printNow(), points.toString());
        SimpleLogger.println();

        configs = null;
        profile = null;

        Runtime.getRuntime().addShutdownHook(new Thread()
        {

            @Override
            public void run()
            {
                SimpleLogger.println();
                SimpleLogger.println("%s > Shutting down...", Shared.printNow());
                List<Future<?>> futures = new ArrayList<>();
                futures.add(Shared.NettyObjects.bossGroup.shutdownGracefully());
                futures.add(Shared.NettyObjects.workerGroup.shutdownGracefully());
                futures.addAll(points.stream().map(SinglePoint::stop).collect(Collectors.toList()));
                futures.forEach(Future::syncUninterruptibly);
                SimpleLogger.println("%s > Done.", Shared.printNow());
                SimpleLogger.println();
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
