package me.lain.muxtun;

import io.netty.util.concurrent.Future;
import me.lain.muxtun.sipo.SinglePoint;
import me.lain.muxtun.sipo.config.SinglePointConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Client {

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private static final List<SinglePoint> thePoints = new ArrayList<>();
    private static List<SinglePointConfig> theConfigs;

    public static void run(Path pathConfig) throws IOException {
        try (BufferedReader in = Files.newBufferedReader(pathConfig, StandardCharsets.UTF_8)) {
            theConfigs = SinglePointConfig.fromJsonList(in.lines()
                    .filter(line -> !line.trim().startsWith("#"))
                    .collect(Collectors.joining(System.lineSeparator())));
        }

        logger.info("Starting...");
        while (true) {
            thePoints.addAll(theConfigs.stream().map(config -> {
                try {
                    return new SinglePoint(config);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()));
            if (Shared.NettyUtils.combineFutures(thePoints.stream().map(SinglePoint::start).collect(Collectors.toList())).awaitUninterruptibly(60L, TimeUnit.SECONDS))
                break;
            Shared.NettyUtils.combineFutures(thePoints.stream().map(SinglePoint::stop).collect(Collectors.toList())).awaitUninterruptibly(60L, TimeUnit.SECONDS);
            thePoints.clear();
            Shared.sleep(5000L);
        }
        logger.info("Done, thePoints are up");

        theConfigs = null;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down...");
            List<Future<?>> futures = new ArrayList<>();
            futures.addAll(Shared.NettyObjects.shutdownGracefully());
            futures.addAll(thePoints.stream().map(SinglePoint::stop).collect(Collectors.toList()));
            Shared.NettyUtils.combineFutures(futures).awaitUninterruptibly(60L, TimeUnit.SECONDS);
        }));
    }

}
