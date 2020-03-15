package me.lain.muxtun.sipo;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import io.netty.channel.Channel;
import me.lain.muxtun.Shared;

class LinkSession
{

    final UUID targetAddress;
    final Function<byte[], Optional<byte[]>> challengeGenerator;
    final Function<byte[], Optional<byte[]>> challengeGenerator_3;
    final Map<UUID, Channel> ongoingStreams;
    final LinkSessionAuthStatus authStatus;
    final LinkSessionConnectionLatencyFactor clf;
    final AtomicInteger pendingOpen;

    LinkSession(SinglePointConfig config)
    {
        targetAddress = config.targetAddress;
        challengeGenerator = question -> {
            if (config.secret != null)
                return Optional.of(Shared.digestSHA256(2, config.secret, question));
            return Optional.empty();
        };
        challengeGenerator_3 = question -> {
            if (config.secret_3 != null && Shared.isSHA3Available())
                return Optional.of(Shared.digestSHA256_3(2, config.secret_3, question));
            return Optional.empty();
        };
        ongoingStreams = new ConcurrentHashMap<>();
        authStatus = new LinkSessionAuthStatus();
        clf = new LinkSessionConnectionLatencyFactor(config.maxCLF);
        pendingOpen = new AtomicInteger();
    }

}
