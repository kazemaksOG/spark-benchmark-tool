set -e


MVN=../spark/build/mvn


$MVN package
$MVN -f ./schedulers/RandomScheduler package
$MVN -f ./schedulers/UserFairScheduler package 
$MVN -f ./schedulers/ShortestFirstScheduler package
