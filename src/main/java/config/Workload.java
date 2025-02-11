package config;

import jobs.Job;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import utils.PoissonWait;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;

enum Frequency {
    SEQ,
    PARA
}

enum InputType {
    PARQUET,
    TXT,
}



public class Workload implements Runnable {
    public enum Partitioning {
        NONE,
        COALESCE,
        REPARTITION,
    }

    private static final System.Logger LOGGER = System.getLogger(User.class.getName());

    private String workloadName;
    private String inputPath;
    private InputType inputType;
    private String className;
    private int totalIterations;
    private transient  int currentIteration;
    private long startTimeMs = 0;
    private double poissonRateInMinutes;
    private Frequency frequency;
    private Partitioning partitioning;

    private long benchStartTime;
    transient SparkSession spark;

    private HashMap<Integer ,HashMap<String, Long>> results;

    public Workload() {
        this.results = new HashMap<>();
        this.currentIteration = 0;
        this.benchStartTime = System.currentTimeMillis();
    }

    public void resetBenchStartTime() {
        this.benchStartTime = System.currentTimeMillis();
    }

    public HashMap<Integer, HashMap<String, Long>> getResults() {
        return results;
    }

    public Partitioning getPartitioning() {
        return partitioning;
    }

    public void setPartitioning(Partitioning partitioning) {
        this.partitioning = partitioning;
    }

    public void setSpark(SparkSession spark) {
        this.spark = spark;
    }

    public String getWorkloadName() {
        return workloadName;
    }

    public void setWorkloadName(String workloadName) {
        this.workloadName = workloadName;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public InputType getInputType() {
        return inputType;
    }

    public void setInputType(InputType inputType) {
        this.inputType = inputType;
    }

    public int getTotalIterations() {
        return totalIterations;
    }

    public void setTotalIterations(int totalIterations) {
        this.totalIterations = totalIterations;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public int getCurrentIteration() {
        return currentIteration;
    }

    public void setCurrentIteration(int currentIteration) {
        this.currentIteration = currentIteration;
    }

    public long getStartTime() {
        return startTimeMs;
    }

    public void setStartTime(long startTime) {
        this.startTimeMs = startTime;
    }

    public Frequency getFrequency() {
        return frequency;
    }

    public void setFrequency(Frequency frequency) {
        this.frequency = frequency;
    }



    public boolean noMoreJobs() {
        currentIteration++;
        return (currentIteration > totalIterations);
    }


    public void run() {
        if(spark == null) {
            LOGGER.log(System.Logger.Level.ERROR,"No spark session defined in workload {}", workloadName);
            return;
        }
        ArrayList<Tuple3<Thread, Job, Integer>> jobList = new ArrayList<>();
        try {
            Job job = (Job) Class.forName(className).getDeclaredConstructor(SparkSession.class, String.class, Partitioning.class).newInstance(spark, inputPath, partitioning);

            spark.sparkContext().setLocalProperty("job.class", job.getClass().getName());

            // variables for setting appropriate job group
            String userName = spark.sparkContext().getLocalProperty("user.name");
            int jobId = 0;

            // Set up poisson distribtuion for waiting
            // if rate is 0, always returns 0

            PoissonWait poissonWait = new PoissonWait(userName + workloadName, poissonRateInMinutes);

            // wait for the job to start
            if(startTimeMs != 0 ) {
                long waitTime = benchStartTime - System.currentTimeMillis() + startTimeMs;
                if(waitTime > 0) {
                    Thread.sleep(waitTime);
                }
            }

            while(!noMoreJobs()) {
                // Adjust the job group for each iteration
                spark.sparkContext().setJobGroup(userName + "_" + workloadName + "_" + jobId, "Job group beloning to user", true);
                Thread jobThread = new Thread(job);
                switch(frequency) {
                    case PARA -> {
                        jobThread.start();
                        // simulate waiting for the user to do something again
                        Thread.sleep(poissonWait.getNextWaitMillis());
                    }
                    case SEQ -> {
                        jobThread.start();
                        // simulate waiting for the user to do something again
                        Thread.sleep(poissonWait.getNextWaitMillis());
                        jobThread.join();
                    }
                }

                jobList.add(new Tuple3<>(jobThread, job, jobId));
                job = (Job) Class.forName(className).getDeclaredConstructor(SparkSession.class, String.class, Partitioning.class).newInstance(spark, inputPath, partitioning);
                jobId++;
            }


        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
                 ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        for(Tuple3<Thread, Job, Integer> job : jobList) {
            try {
                job._1().join();
                results.put(job._3(), job._2().getResults());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        spark.sparkContext().setLocalProperty("JobType", null);
        spark.sparkContext().clearJobGroup();

    }

    @Override
    public String toString() {
        return "config.Workload{" +
                "workloadName='" + workloadName + '\'' +
                ", inputPath='" + inputPath + '\'' +
                ", inputType=" + inputType +
                ", totalIterations=" + totalIterations +
                ", currentIteration=" + currentIteration +
                ", startTime='" + startTimeMs + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
