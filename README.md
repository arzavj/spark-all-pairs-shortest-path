# spark-all-pairs-shortest-path

command to run Spark on the cluster
 ~/Dropbox/CME323/cme323.pem target/scala-2.10/all-pairs-shortest-path_2.10-1.0.jar root@54.146.149.83:~/

./bin/spark-submit --class AllPairsShortestPath --master spark://ec2-54-146-149-83.compute-1.amazonaws.com:7077 --deploy-mode cluster hdfs://ec2-54-146-149-83.compute-1.amazonaws.com:9010/vol/all-pairs-shortest-path_2.10-1.0.jar


