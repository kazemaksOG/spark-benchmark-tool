# Spark benchmarking tool
This is a tool used for testing the Spark framework itself rather than the underlying system. For this particular project, it is meant to test and see how different schedulers affect throughput and fairness of the application.


## Structure
* Main - used for testing around. Can be submitted the same way BenchRunner, just replace BenchRunner class with Main when launching.


## Setup
Scriptes have been tested on DAS5 and require `bash` to be the shell sourcing them. These scripts depend on the following components to be present.

* **DAS5 big data deployment framework** - To deploy Spark on DAS5, "Deployment scripts for big data frameworks on DAS-5" framework was used and lightly extended. This can be be found here: [https://github.com/kazemaksOG/das-bigdata-deployment-python3](https://github.com/kazemaksOG/das-bigdata-deployment-python3)

* **Custom schedulers** - Custom schedulers are under the `schedulers` directory. They have to be packed and added as arguments to Spark launcher

* **Custom spark** - A modified Spark version is used to run these tests. This can be found in here: [https://github.com/kazemaksOG/spark-3.5.5-custom#](https://github.com/kazemaksOG/spark-3.5.5-custom#)

To setup the environment for the scripts:
1. Clone the DAS5 deployment framework 
```
git clone https://github.com/kazemaksOG/das-bigdata-deployment-python3.git
```
2. Clone the Custom spark inside 
```
cd das-bigdata-deployment/frameworks
git clone https://github.com/kazemaksOG/spark-3.5.5-custom.git
```
3. Compile custom spark with either `sbt` or `mvn` provided in `build`. **Note:** `sbt` is much faster to compile, but I found that sometimes there are some dependencies issues that magically are resolved by first compiling using `mvn`.
```
cd spark-3.5.5-custom
./build/mvn -DskipTests package 
# or ./build/sbt package
```
Optional: If using a different spark version, modify das deployer `big_data_deployer/spark.py` to have a line on the bottom that adds the coresponing spark version. Template dir are settings used in conf/spark/<TEMPLATE DIR>. 
```python
get_framework_registry().framework("spark").add_version(SparkFrameworkVersion(<VERSION>, <GIT LINK OR ARCHIVE>, <GIT OR TGZ>, <NAME_OF_ROOD_DIR>, <TEMPLATE DIR>)
```
5. Add jar file to the maven repository
```
export $SPARK_HOME=$(pwd)
./build/mvn install:install-file -Dfile=$SPARK_HOME/core/target/scala-2.12/spark-core_2.12-3.5.5.jar -DgroupId=org.apache.spark -DartifactId=spark-core_2.12 -Dversion=3.5.5-custom -Dpackaging=jar
```
5. Compile the benchmarking tool and schedulers using `compile_mvn.sh`. May need to modify the mvn location at the top of the script.
```
bash compile_mvn.sh
```


6. Modify the `Deployer variables` on top of `setup_cluster.sh` script and source it. Can optionally supply the reservation ID if already made.
```
source setup_cluster.sh <OPTIONAL_RESERVATION_ID>
```
7. Modify benchmarks, spark configs, schedulers and paths on top of `run_all_benchmarks.sh` script and source it.
```
source run_all_benchmarks.sh RESERVATION_ID
```

8. Follow instrucitons printed

## Running

## Results
Results can be obtained by running the script in `results/visualize_results.py`. It relies on the output results from the benchmarks and the history server running on localhost to get even data. To setup the running environment:

1. Get the benchmark output from `$PROJECT_ROOT/target/becnh_outputs` and place them somewhere in the `$PROJECT_ROOT/results` directory
2. Change the paths in `visualize_results.py` to reflect that location.
3. Gather the events from the benchmarks and have the history server running in the background on them.
4. launch the python script with `python3 visualize_results <COMMAND>`



