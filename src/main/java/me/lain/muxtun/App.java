package me.lain.muxtun;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

public class App
{

    public static class SinglePointConfig
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
        public int numLinks = SinglePoint.DEFAULT_NUMLINKS;
        public int limitOpen = SinglePoint.DEFAULT_LIMITOPEN;
        public int maxCLF = SinglePoint.DEFAULT_MAXCLF;
        public SslContext sslCtx = null;
        public Optional<byte[]> secret = null;
        public Optional<byte[]> secret_3 = null;

        public SinglePointConfig(String name)
        {
            this.name = name;
        }

    }

    private static Map<String, SinglePointConfig> configs = new HashMap<>();
    private static List<SinglePoint> points = new ArrayList<>();
    private static String profile = "SinglePoint";

    private static void discardOut()
    {
        System.setOut(new PrintStream(Shared.voidStream));
        System.setErr(new PrintStream(Shared.voidStream));
    }

    private static Optional<byte[]> generateSecret(Path path, byte[] magic)
    {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ))
        {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            fc.transferTo(0L, Long.MAX_VALUE, Channels.newChannel(new DigestOutputStream(Shared.voidStream, md)));

            return Optional.of(md.digest(magic));
        }
        catch (Exception e)
        {
            return Optional.empty();
        }
    }

    private static Optional<byte[]> generateSecret_3(Path path, byte[] magic)
    {
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ))
        {
            MessageDigest md = MessageDigest.getInstance("SHA3-256");

            fc.transferTo(0L, Long.MAX_VALUE, Channels.newChannel(new DigestOutputStream(Shared.voidStream, md)));

            return Optional.of(md.digest(magic));
        }
        catch (Exception e)
        {
            return Optional.empty();
        }
    }

    private static SinglePointConfig getConfig(String profile)
    {
        return configs.computeIfAbsent(profile, SinglePointConfig::new);
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
            logOut(pathLog.orElse(FileSystems.getDefault().getPath("MuxTunnel.log")));

        return pathConfig.orElse(FileSystems.getDefault().getPath("MuxTunnel.cfg"));
    }

    private static void logOut(Path pathLog) throws IOException
    {
        final OutputStream fileOut = new BufferedOutputStream(Channels.newOutputStream(FileChannel.open(pathLog, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)));

        System.setOut(new PrintStream(new OutputStream()
        {

            final OutputStream original = System.out;

            @Override
            public void close() throws IOException
            {
                original.close();
                fileOut.close();
            }

            @Override
            public void flush() throws IOException
            {
                original.flush();
                fileOut.flush();
            }

            @Override
            public void write(byte b[]) throws IOException
            {
                original.write(b);
                fileOut.write(b);
            }

            @Override
            public void write(byte b[], int off, int len) throws IOException
            {
                original.write(b, off, len);
                fileOut.write(b, off, len);
            }

            @Override
            public void write(int b) throws IOException
            {
                original.write(b);
                fileOut.write(b);
            }

        }, true, StandardCharsets.UTF_8.name()));

        System.setErr(new PrintStream(new OutputStream()
        {

            final OutputStream original = System.err;

            @Override
            public void close() throws IOException
            {
                original.close();
                fileOut.close();
            }

            @Override
            public void flush() throws IOException
            {
                original.flush();
                fileOut.flush();
            }

            @Override
            public void write(byte b[]) throws IOException
            {
                original.write(b);
                fileOut.write(b);
            }

            @Override
            public void write(byte b[], int off, int len) throws IOException
            {
                original.write(b, off, len);
                fileOut.write(b, off, len);
            }

            @Override
            public void write(int b) throws IOException
            {
                original.write(b);
                fileOut.write(b);
            }

        }, true, StandardCharsets.UTF_8.name()));
    }

    public static void main(String[] args) throws Exception
    {
        try (BufferedReader in = Files.newBufferedReader(init(args)))
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
                    getConfig(profile).trustSha1.add(value);
                    System.out.println(String.format("%s > [%s] %s has been added to %s", Shared.printNow(), profile, value, "trustSha1"));
                }
                else if ("ciphers".equals(name))
                {
                    List<String> toAdd = Arrays.asList(value.split(":"));
                    getConfig(profile).ciphers.addAll(toAdd);
                    for (String c : toAdd)
                        System.out.println(String.format("%s > [%s] %s has been added to %s", Shared.printNow(), profile, c, "ciphers"));
                }
                else if ("protocols".equals(name))
                {
                    List<String> toAdd = Arrays.asList(value.split(":"));
                    getConfig(profile).protocols.addAll(toAdd);
                    for (String c : toAdd)
                        System.out.println(String.format("%s > [%s] %s has been added to %s", Shared.printNow(), profile, c, "protocols"));
                }
                else if ("targetAddress".equals(name))
                {
                    getConfig(profile).targetAddress = UUID.fromString(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "targetAddress", getConfig(profile).targetAddress.toString()));
                }
                else if ("pathSecret".equals(name))
                {
                    getConfig(profile).pathSecret = FileSystems.getDefault().getPath(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "pathSecret", getConfig(profile).pathSecret.toString()));
                }
                else if ("pathSecret_3".equals(name))
                {
                    getConfig(profile).pathSecret_3 = FileSystems.getDefault().getPath(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %s", Shared.printNow(), profile, "pathSecret_3", getConfig(profile).pathSecret_3.toString()));
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
                else if ("maxCLF".equals(name))
                {
                    getConfig(profile).maxCLF = Integer.parseInt(value);
                    System.out.println(String.format("%s > [%s] %s has been set to %d", Shared.printNow(), profile, "maxCLF", getConfig(profile).maxCLF));
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
                if (config.trustSha1.isEmpty())
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "trustSha1"));
                    failed = true;
                }
                if (config.targetAddress == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s", Shared.printNow(), config.name, "targetAddress"));
                    failed = true;
                }
                if (config.pathSecret == null && config.pathSecret_3 == null)
                {
                    System.out.println(String.format("%s > [%s] Missing %s and %s", Shared.printNow(), config.name, "pathSecret", "pathSecret_3"));
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
                if (config.maxCLF < 0)
                {
                    System.out.println(String.format("%s > [%s] Invalid %s", Shared.printNow(), config.name, "maxCLF"));
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
                config.sslCtx = SslContextBuilder.forClient().trustManager(new FingerprintTrustManagerFactory(config.trustSha1)).ciphers(!config.ciphers.isEmpty() ? config.ciphers : null, SupportedCipherSuiteFilter.INSTANCE).protocols(!config.protocols.isEmpty() ? config.protocols : null).build();
            System.out.println(String.format("%s > Done.", Shared.printNow()));

            System.out.println();
            System.out.println(String.format("%s > Generating Secret...", Shared.printNow()));
            for (SinglePointConfig config : configs.values())
            {
                config.secret = generateSecret(config.pathSecret, Shared.magic);
                config.secret_3 = generateSecret_3(config.pathSecret_3 != null ? config.pathSecret_3 : config.pathSecret, Shared.magic);
            }
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
                    config.secret,
                    config.secret_3,
                    config.numLinks,
                    config.limitOpen,
                    config.maxCLF,
                    config.name));
        System.out.println(String.format("%s > Done. %s", Shared.printNow(), points.toString()));
        System.out.println();

        configs = null;
        profile = null;

        Runtime.getRuntime().addShutdownHook(new Thread()
        {

            @Override
            public void run()
            {
                System.out.println();
                System.out.println(String.format("%s > Shutting down...", Shared.printNow()));
                Shared.NettyObjects.bossGroup.shutdownGracefully().syncUninterruptibly();
                Shared.NettyObjects.workerGroup.shutdownGracefully().syncUninterruptibly();
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
