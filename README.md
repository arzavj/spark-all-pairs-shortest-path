# spark-all-pairs-shortest-path

## make jar

cd spark-all-pairs-shortest-path/
sbt package

## command to run Spark on the cluster
scp -i keyname.pem target/scala-2.10/all-pairs-shortest-path_2.10-1.0.jar root@XX.XXX.XXX.XX:~/

ssh -i keyname.pem root@XX.XX

## now put the jar in the hdfs
./persistent-hdfs/bin/hadoop fs -rm /vol/all-pairs-shortest-path_2.10-1.0.jar
./persistent-hdfs/bin/hadoop fs -put all-pairs-shortest-path_2.10-1.0.jar hdfs://ec2-54-146-149-83.compute-1.amazonaws.com:9010/vol

cd spark

./bin/spark-submit --class AllPairsShortestPath --master spark://ec2-54-146-149-83.compute-1.amazonaws.com:7077 \
 --deploy-mode cluster hdfs://ec2-54-146-149-83.compute-1.amazonaws.com:9010/vol/all-pairs-shortest-path_2.10-1.0.jar
 
 
 TODOs:
 1. ask Rezar (distance first / procedures)
 2. design API
 2. implement API + commenting + cleaning the code
 3. write unit tests
 4. write readme file

