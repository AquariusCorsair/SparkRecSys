import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating


object HyBridRec {
	def enConf() = {
		val conf = new SparkConf().setAppName("ALSCF")
    	val sc = new SparkContext(conf)
    	sc
	}

	def str2Int(s : String) :Int = {
		val num = s match{
			case "Action" => 0
			case "Adventure" => 1
			case "Animation" => 2
			case "Children's" => 3
			case "Comedy" => 4
			case "Crime" => 5
			case "Documentary" => 6
			case "Drama" => 7
			case "Fantasy" => 8
			case "Film-Noir" => 9
			case "Horror" => 10
			case "Musical" => 11
			case "Mystery" => 12
			case "Romance" => 13
			case "Sci-Fi" => 14
			case "Thriller" => 15
			case "War" => 16
			case "Western" => 17
		}
		num
	}

	def computeMSE(model: MatrixFactorizationModel, data: RDD[Rating]) = {
	    val predictions = getPredictions (model, data)
	    val MSE = computeMSE2(data, predictions)
	    MSE
	  }
	def getPredictions(model: MatrixFactorizationModel, data: RDD[Rating]) : RDD[((Int, Int), Double)] = {
		val usersProducts = data.map { case Rating(user, product, rate) =>
	      (user, product)
	    }
	    val predictions = model.predict(usersProducts).map {
	      case Rating(user, product, rate) => ((user, product), rate)
	    }
	    predictions
	}
	def computeMSE2(original: RDD[Rating], predictions: RDD[((Int, Int), Double)]) = {
		val UMR1R2 = original.map { case Rating(user, movie, rate) =>
	      ((user, movie), rate)
	    }.join(predictions)
	    val MSE = UMR1R2.map { case ((user, product), (r1, r2)) =>
	      val err = r1 - r2
	      err*err
	    }.mean()
	    MSE
	}

	def UMR2UTR(MT: RDD[(Int,String)],UMR: RDD[Rating]): RDD[Rating] = {
		val r1 = UMR.map{case Rating(user, product, rate) => (product.toInt,(user.toInt,rate.toDouble))}
		val UTR = MT.join(r1).map{case (movieId,(tag,(user,rate))) => 
			((user,tag),rate) 
		}.mapValues(x=>(x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).mapValues{
			case (x,y) => x/y
			}.map{
				case ((user, tag),rate) => Rating(user,str2Int(tag),rate)
				}
		UTR
	}

	def getCountError(testUMR: RDD[Rating], UMP: RDD[((Int, Int), Double)]) : RDD[(Int, Double)] = {
		val MC = testUMR.map{case Rating(user, movie, rate) => (movie, 1)}.reduceByKey((x, y) => (x+y))
		val UMRP = testUMR.map { case Rating(user, movie, rate) =>
	      ((user, movie), rate)
	    }.join(UMP)
	    val UME = UMRP.map { case ((user, product), (r1, r2)) =>
	      ((user, product), (r1-r2)*(r1-r2))
	    }
	    val CEE = UME.map{case ((user, movie), error) => (movie, error)}. join(MC).map{
	    	case (movie, (error, count)) => (count, error)
	    }
	    val CE = CEE.mapValues(x=>(x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).mapValues{case (x,y) => x/y}
	    CE
	}

	def main(args: Array[String]){
		// Coinfigure env
		val sc = enConf()

		// Load and parse the data  => trainUMR & testUMR & UT
		val data = sc.textFile("data/ml-1m/ratings.dat")
		val UMR = data.map(_.split("::").take(3) match { 
			case Array(user, movie, rate) => 
			Rating(user.toInt, movie.toInt, rate.toDouble)
		})
		val (trainUMR, testUMR) = UMR.randomSplit(Array(0.5,0.5)) match {
			case Array(train, test) => (train, test)
		}
		testUMR.persist

		//UMR + MT => UTR
		val mData = sc.textFile("data/ml-1m/movies.dat")
		val MT = mData.map(_.split("::").take(3) match { 
			case Array(movie, name, tags) => (movie.toInt,tags)
		}).flatMapValues(tags => tags.split('|')).persist
		val trainUTR = UMR2UTR(MT,trainUMR)
		val testUTR = UMR2UTR(MT,testUMR)

		// Build the recommendation model using ALS
		val rank = 10
		val numIterations = 10
		val modelUM = ALS.train(trainUMR, rank, numIterations, 0.01)
		val modelUT = ALS.train(trainUTR, rank, numIterations, 0.01)

		// Evaluate MSE for the original model
		val UMP1 = getPredictions(modelUM, testUMR).persist
		val MSEUM = computeMSE2(testUMR,UMP1)
		val MSEUT = computeMSE(modelUT, testUTR)

		// Predict rate(i,j)
		// var u1=sc.parallelize(Array(6045,2,3).map(f=>(f,10)))
		// var p1 = model.predict(u1).map{ case Rating(user, product, rate) => (rate)}.collect()
		// println("***************************************\n")
		// println("prediction of movie 10 for user 6045: " + p1(0))
		// println("prediction of movie 10 for user 2: " + p1(1))
		// println("***************************************\n")		

		
		// Predict UMP2 using modelUT, testUMR and MT
		// testUMR => MU(movie, user)
		val MU = testUMR.map { case Rating(user, movie, rate) => (movie, user)}
		//create MUT(movie,(user,tag)) from MU and MT
		val MTInt = MT.map{case (movie,tag) => (movie,str2Int(tag))}
		val MUT = MU.join(MTInt)
		//transite MUT to UMPP((user,movie),predictionBasedOnOneTag) using modelUT
		val UT = MUT.map{case (movie,(user,tag)) => (user,tag)}
		val P = modelUT.predict(UT).map{
	    	case Rating(user, tag, rate) => ((user, tag), rate)
	    }
	    val UMPOneTag = MUT.map{
	    	case (movie,(user,tag)) => ((user,tag),movie)
	    	}.join(P).map{
	    		case ((user,tag),(movie,rate)) => ((user,movie),rate)
	    	}
	    //reduce UMPOneTag by M => UMP2
	    val UMP2 = UMPOneTag.mapValues(x=>(x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2)).mapValues{case (x,y) => x/y}.persist
	 	// println("****************************MUP2\n")
		// UMP2.collect.foreach(println)
		// println("***************************************\n")

		// Calculate MSEUM2 for UMR2 using testUMR
		val MSEUM2 = computeMSE2(testUMR, UMP2)
		println("***************************************\n")
		println("modelUM Mean Squared Error = " + MSEUM)
		println("modelUT Mean Squared Error = " + MSEUT)
		println("modelUM2 Mean Squared Error = " + MSEUM2)
		println("***************************************\n")

		// get CE(count,MSE) from UMP, testUMR
		// val testMUR = testUMR.map{case Rating(user,movie,rate) => Rating(movie,user,rate)}
		// val MUP1 = UMP1.map{case ((user,movie),prediction) => ((movie,user),prediction)}
		// val MUP2 = UMP2.map{case ((user,movie),prediction) => ((movie,user),prediction)}
		val CE1 = getCountError(testUMR, UMP1).sortByKey()
		val CE2 = getCountError(testUMR, UMP2).sortByKey()
		CE1.map(a => a._1.toString + ',' + a._2.toString + '\n').saveAsTextFile("target/tmp/CE1")
		CE2.map(a => a._1.toString + ',' + a._2.toString + '\n').saveAsTextFile("target/tmp/CE2")
		// CE1.take(100).foreach(println)
		// CE2.take(100).foreach(println)


		// Save and load model
		// modelUT.save(sc, "target/tmp/myCollaborativeFilter")
		// val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")


		//To-do list:
		//(4) determine the weight w for r(i,j)= w*r1(i,j) +(1-w)*r2(i,j) based on movie(j)'s ratedCount
	}
}
