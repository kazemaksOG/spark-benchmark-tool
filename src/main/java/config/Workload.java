package config;

import jobs.Job;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
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
    private String workloadName;
    private String inputPath;
    private InputType inputType;
    private String className;
    private int totalIterations;
    private transient  int currentIteration;
    private long startTimeMs = 0;
    private double poissonRateInMinutes;
    private Frequency frequency;

    private long benchStartTime;
    transient SparkSession spark;

    private HashMap<Integer ,HashMap<String, Long>> results;

    public Workload() {
        this.results = new HashMap<>();
        this.currentIteration = 0;
        this.benchStartTime = System.currentTimeMillis();
    }

    public HashMap<Integer, HashMap<String, Long>> getResults() {
        return results;
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
            System.err.println("No spark session defined in workload " + workloadName);
            return;
        }
        ArrayList<Tuple2<Thread, Job>> jobList = new ArrayList<>();
        try {
            Job job = (Job) Class.forName(className).getDeclaredConstructor(SparkSession.class, String.class).newInstance(spark, inputPath);
            spark.sparkContext().setLocalProperty("job.class", job.getClass().getName());
            spark.sparkContext().setJobGroup(workloadName, job.getClass().getName(), true);

            // wait for the job to start
            if(startTimeMs != 0 ) {
                long waitTime = benchStartTime - System.currentTimeMillis() + startTimeMs;
                if(waitTime > 0) {
                    Thread.sleep(waitTime);
                }
            }

            // Set up poisson distribtuion for waiting
            // if rate is 0, always returns 0
            PoissonWait poissonWait = new PoissonWait(workloadName, poissonRateInMinutes);

            while(!noMoreJobs()) {
                Thread jobThread = new Thread(job);
                switch(frequency) {
                    case PARA -> {
                        jobThread.start();
                    }
                    case SEQ -> {
                        jobThread.start();
                        // simulate waiting for the user to do something again
                        Thread.sleep(poissonWait.getNextWaitMillis());
                        jobThread.join();
                    }
                }

                jobList.add(new Tuple2<>(jobThread, job));
                job = (Job) Class.forName(className).getDeclaredConstructor(SparkSession.class, String.class).newInstance(spark, inputPath);
            }


        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
                 ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        int jobId = 0;
        for(Tuple2<Thread, Job> job : jobList) {
            try {
                job._1.join();
                results.put(jobId, job._2.getResults());
                jobId++;
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
