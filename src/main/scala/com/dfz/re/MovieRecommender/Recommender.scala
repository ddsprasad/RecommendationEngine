/**
  * Recommender Class recommends the movies to user using recommendProducts
  * and predicts the ratings of the movies not rated by user
  */
package com.dfz.re.MovieRecommender

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

//class Recommender(@transient sc: SparkContext, hist_ratingFile: String,movies_file:String,ratingsdetailsRDD:RDD[String],rec_userId:Int) extends Serializable {
//
class Recommender(@transient sc: SparkContext,unionRDD:RDD[String],movies_file:String,rec_userId:Int) extends Serializable {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    //Taking movies data to map recommendProducts result to movies data
    val movies_data = sc.textFile(movies_file)
    val movie_header = movies_data.first()
    val movies = movies_data.filter(x=>x != movie_header)
    // taking movieID and title from movies data and storing as map
    val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt,  array(1))).collectAsMap()

    def parseRating (str: String): Rating = {
        val fields = str.split(",")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }
    //splitting the joined data and mapping with mllib Rating class
    val rating = unionRDD.map(parseRating).cache()

    //splitting the joined into training data and test data
    val splits = rating.randomSplit(Array(0.8, 0.2), 0L)

    //the first 80% of data is taken as training data
    val trainingRatingsRDD = splits(0).cache()

    //The rest 20% of data is used as test data
    val testRatingsRDD = splits(1).cache()

    val model = new ALS().setRank(20).setIterations(10).run(trainingRatingsRDD)


    //getting top 5 recommendations for the user 10616
    val topRecsForUser = model.recommendProducts(rec_userId,5)

    //mapping movies recommended to movies title
    val topNRecs = topRecsForUser.map(rating => (rating.user,titles(rating.product),rating.rating))

    //converting the array of recommendations to DataFrame
    val topRecsRDD=sc.parallelize(topNRecs).toDF("userid","title","rating")

    //dumping the top recommendations to cassandra
    topRecsRDD.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "top_n_recommendations")).save()

    //predicting the rating for the movieID 2 for user 10616
    val rating_predict= model.predict(rec_userId, 2)

    val ratings_predictDF=sc.parallelize(List(rating_predict)).toDF("rating")

    //dumping the predicted ratings to cassandra
    ratings_predictDF.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "predict_ratings")).save()

    //Testing
    val testUserProductRDD = testRatingsRDD.map {
      case Rating(userId, movieId, rating) => (userId, movieId)
    }

    val predictionsForTestRDD = model.predict(testUserProductRDD)

    val result=predictionsForTestRDD.take(2)

   println("test data result is"+result.mkString("\n"))

}