# Spark Movie Recommendation System

## Discription
This project uses two ways to build a movie recommendation system in spark. The goal for this project is:
* Implement our `Double ALS` algorithm in movie recommendation.
* Compare our `Double ALS` algorithm with the oringinal ALS algorithm presented by spark library.
* Prove that, for not so popular movies, i.e. thoes are rated by fewer users, our algorithm works better.

## Installation
* Prerequisites
```
Install spark 2.0.1 from source
Export $SPARK_HOME to PATH 
```

* Modify `build.sbt` file based on your installation version of scala and spark.
* Compile and generate the jar file with `build.sbt`: 
	> sbt package

  Result:
```
  ...
  [info] Packaging /.../target/scala-2.11/hybridrec_2.11-1.0.jar 
  ...
```

* Use `spark-submit` to locally run the generated jar file with 4 core: 

	> $SPARK_HOME/bin/spark-submit --master local[4] target/scala-2.11/hybridrec_2.11-1.0.jar

  The result is stored in folder ./target/tmp/CE1 and ./target/tmp/CE2
  - ./target/tmp/CE1 data structure: `<count,MSE>`. `count` n is the number of rating . And `MSE` is the average mean square error (MSE) of movies that was rated by n users in the training data, using oringinal spark ALS library.
  - ./target/tmp/CE2 data structure: `<count,MSE>`. `count` n is the number of rating . And `MSE` is the average mean square error (MSE) of movies that was rated by n users in the training data, using our double ALS algorithm. 

## Detailed Report
Basically, our implementation is summerised in this image:
![alt text](https://github.com/hualiu01/SparkRecognitionSystem/tree/master/images/spark.png "Logo Title Text 1")
For more detailed report:
- [Paper](https://github.com/hualiu01/SparkRecognitionSystem/blob/master/report/double-matrix-factorization%20.pdf)
- [demo](https://github.com/hualiu01/SparkRecognitionSystem/blob/master/report/CloudCompluting%20final%20presentation.pptx.pdf) 
