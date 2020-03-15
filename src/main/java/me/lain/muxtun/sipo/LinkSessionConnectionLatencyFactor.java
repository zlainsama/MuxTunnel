package me.lain.muxtun.sipo;

import java.util.Optional;

class LinkSessionConnectionLatencyFactor
{

    private volatile Long startTime;
    private volatile Integer factor;

    final int maxCLF;

    LinkSessionConnectionLatencyFactor(int maxCLF)
    {
        this.maxCLF = maxCLF;
    }

    Optional<Boolean> completeCalculation()
    {
        if (startTime != null)
        {
            synchronized (this)
            {
                if (startTime != null)
                {
                    factor = (int) (Math.min(Math.max(0L, System.currentTimeMillis() - startTime), 1000L) / 125L);
                    startTime = null;
                }
            }
        }

        return Optional.ofNullable(factor).map(clf -> clf > maxCLF);
    }

    Optional<Integer> getFactor()
    {
        return Optional.ofNullable(factor);
    }

    boolean initiateCalculation()
    {
        if (startTime == null)
        {
            synchronized (this)
            {
                if (startTime == null)
                {
                    startTime = System.currentTimeMillis();
                    return true;
                }
            }
        }

        return false;
    }

}
