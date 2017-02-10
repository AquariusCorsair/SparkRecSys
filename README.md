# Prerequisites
* install hadoop 2.7.1 single node cluster on unbuntu 14.10
* install spark 2.0.1 from source
* export $SPARK_HOME tp PATH 

# Build And Run
* Modify 'build.sbt' file based on your installation version of scala and spark
* compile and generate the jar file: 
	$ sbt package
  -------------------
  ...
  [info] Packaging /.../target/scala-2.11/hybridrec_2.11-1.0.jar ...
  -------------------
* Use spark-submit to locally run the generated jar file with 4 core: 
	$SPARK_HOME/bin/spark-submit --master local[4] target/scala-2.11/hybridrec_2.11-1.0.jar
  -------------------
  The result is stored in folder ./target/tmp/CE1 and ./target/tmp/CE1
  * ./target/tmp/CE1 data structure: <count,MSE>. 'count' n is the number of rating . And 'MSE' is the average mean square error (MSE) of movies that was rated by n users in the training data, using oringinal spark ALS library.
  * ./target/tmp/CE1 data structure: <count,MSE>. 'count' n is the number of rating . And 'MSE' is the average mean square error (MSE) of movies that was rated by n users in the training data, using our double ALS algorithm.
  

# Detailed Report
paper:
Slides:
