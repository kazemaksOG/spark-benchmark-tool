package utils;


import java.util.Random;

public class PoissonWait {
    Random rand = new Random();
    static double  MINUTE_TO_MILLIS = 60 * 1000;
    double lambda;
    public PoissonWait(String jobName, double lambda) {
        rand.setSeed(jobName.hashCode());
        this.lambda = lambda;
    }

    /**
     *
     * @return returns the wait time in milliseconds (period assumed to be a minute)
     */
    public long getNextWaitMillis() {
        if(lambda == 0) {
            return 0;
        }
        return (long)((-Math.log(rand.nextDouble()) / lambda) * MINUTE_TO_MILLIS);
    }


}
