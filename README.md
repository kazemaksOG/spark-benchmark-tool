# Spark benchmarking tool
This is a tool used for testing the Spark framework itself rather than the underlying system. For this particular project, it is meant to test and see how different schedulers affect throughput and fairness of the application.


## Structure
* Main - used for testing around. Can be submitted the same way BenchRunner, just replace BenchRunner class with Main when launching.


## Setup
Scriptes have been tested on DAS5 and require `bash` to be the shell sourcing them.

### Custom schedulers

Compile spark locally with mvn package or sbt package, then locate the created jar file and add it to mvn repository
```
 mvn install:install-file -Dfile=$SPARK_HOME/core/target/scala-2.13/spark-core_2.13-4.0.0-SNAPSHOT.jar -DgroupId=org.apache.spark -DartifactId=spark-core_2.12 -Dversion=3.5.4-custom -Dpackaging=jar
```





## Results
Results can be obtained by running the script in `results/visualize_results.py`. It relies on the output results from the benchmarks and the history server running on localhost to get even data. To setup the running environment:

1. Get the benchmark output from `$PROJECT_ROOT/target/becnh_outputs` and place them somewhere in the `$PROJECT_ROOT/results` directory
2. Change the paths in `visualize_results.py` to reflect that location.
3. Gather the events from the benchmarks and have the history server running in the background on them.
4. launch the python script with `python3 visualize_results <COMMAND>`



