package me.lain.muxtun.sipo;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.UUID;

class RandomSession
{

    private final UUID sessionId;
    private final byte[] challenge;

    RandomSession()
    {
        sessionId = UUID.randomUUID();
        challenge = new byte[512];

        try
        {
            SecureRandom.getInstanceStrong().nextBytes(challenge);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new Error(e);
        }
    }

    byte[] getChallenge()
    {
        return challenge;
    }

    UUID getSessionId()
    {
        return sessionId;
    }

}
