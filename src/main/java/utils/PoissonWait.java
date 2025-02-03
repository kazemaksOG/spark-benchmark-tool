package utils;

import org.apache.commons.math3.distribution.UniformRealDistribution;

public class PoissonWait {
    UniformRealDistribution uniformDistribution;
    static double  MINUTE_TO_MILLIS = 60 * 1000;
    double lambda;
    public PoissonWait(String jobName, double lambda) {
        uniformDistribution = new UniformRealDistribution();
        uniformDistribution.reseedRandomGenerator(jobName.hashCode());
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
        return (long)((-Math.log(uniformDistribution.sample()) / lambda) * MINUTE_TO_MILLIS);
    }


}
