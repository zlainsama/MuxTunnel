package me.lain.muxtun;

import io.netty.util.concurrent.Future;
import me.lain.muxtun.sipo.SinglePoint;
import me.lain.muxtun.sipo.config.SinglePointConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private static final Logger LOGGER = LogManager.getLogger();

    private static final List<SinglePoint> thePoints = new ArrayList<>();

    static {
        ShutdownTasks.register(() -> {
            if (!thePoints.isEmpty()) {
                LOGGER.info("Shutting down...");
                shutdown(60L, TimeUnit.SECONDS);
                LOGGER.info("Done.");
            }

            LogManager.shutdown();
        });
    }

    public static void run(Path pathConfig) throws IOException {
        List<SinglePointConfig> theConfigs;
        try (BufferedReader in = Files.newBufferedReader(pathConfig, StandardCharsets.UTF_8)) {
            theConfigs = SinglePointConfig.fromJsonList(in.lines()
                    .filter(line -> !line.trim().startsWith("#"))
                    .collect(Collectors.joining(System.lineSeparator())));
        }

        LOGGER.info("Starting...");
        Future<?> result;
        while (true) {
            thePoints.addAll(theConfigs.stream().map(config -> {
                try {
                    return new SinglePoint(config);
                } catch (IOException e) {
                    LOGGER.fatal("error starting thePoints", e);
                    LogManager.shutdown();
                    System.exit(1);
                    return null;
                }
            }).collect(Collectors.toList()));
            if ((result = Shared.NettyUtils.combineFutures(thePoints.stream().map(SinglePoint::start).collect(Collectors.toList()))).awaitUninterruptibly(60L, TimeUnit.SECONDS))
                break;
            Shared.NettyUtils.combineFutures(thePoints.stream().map(SinglePoint::stop).collect(Collectors.toList())).awaitUninterruptibly(60L, TimeUnit.SECONDS);
            thePoints.clear();
            Shared.sleep(5000L);
            LOGGER.error("Took too long to start, retrying...");
        }
        if (result.isSuccess())
            LOGGER.info("Done, thePoints are up.");
        else {
            LOGGER.fatal("error starting thePoints", result.cause());
            LogManager.shutdown();
            System.exit(1);
        }
    }

    public static void shutdown(long timeout, TimeUnit unit) {
        List<Future<?>> futures = new ArrayList<>();
        futures.addAll(Shared.NettyObjects.shutdownGracefully());
        futures.addAll(thePoints.stream().map(SinglePoint::stop).collect(Collectors.toList()));
        Shared.NettyUtils.combineFutures(futures).awaitUninterruptibly(timeout, unit);
    }

}
