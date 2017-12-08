/**
  * Kafka Consumer is Spark Streaming Job which is pointed to the
  * Kafka topic where producer sends the data of movies dataset
  * and calls recommender object to recommend movies to user.
  */
package com.dfz.re.Consumer

import com.dfz.re.Model.{Ratings, movies}
import com.dfz.re.MovieRecommender.Recommender
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable._

object KafkaConsumerSpark extends App {

  if(args.length != 6) {
    System.err.println("Usage: " + this.getClass.getSimpleName + " <movies_topic> <ratings_topic> <hist_ratingFile> <movies_File> <rec_userId> <BrokerNum>")
    System.exit(1)
  }

  val appName = "KafkaConsumerSpark"
  val topic_movies = args(0).toString
  val topic_ratings = args(1).toString
  //path of the ratings historical data
  val hist_ratingFile=args(2).toString
  val movies_file=args(3).toString
  //recommendations for this particular userID
  val rec_userId=args(4).toInt

  //Initialising Spark Context
  val conf=new SparkConf()
          .setAppName(appName)
          .setMaster("local[*]")
          .set("spark.cores.max", "2")
          .set("spark.executor.memory", "1g")
          .set("spark.rdd.compress", "true")
          .set("spark.storage.memoryFraction", "1")
          .set("spark.streaming.unpersist", "true")
          .set("spark.sql.thriftserver.scheduler.pool","accounting")
          .set("spark.sql.shuffle.partitions","10")
          .set("spark.cassandra.connection.host", "10.1.51.42")
  val sc=new SparkContext(conf)

  //Initializing SqlContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  //Initialising Spark Streaming Context
  val ssc = new StreamingContext(sc, Seconds(10))

  //List of kafka Topics to be consumed
  val kafkat = List(topic_movies, topic_ratings)

  /*Define properties for how the Consumer finds the cluster and
  deserialize the messages */
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> args(5).toString,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "group.id" ->"something",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //converting historical ratings data to Dataframe
  val hist_ratingsRDD= sc.textFile(hist_ratingFile)
  val hist_ratingdf = hist_ratingsRDD.map(_.split(",")).map(
    payload => {
      Ratings(
        payload(0).toInt,
        payload(1).toInt,
        payload(2).toDouble
      )
    }
  ).toDF()

  //Kafka Spark Streaming Consumer
  val kafkaStream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](kafkat, kafkaParams)
  )

  kafkaStream.foreachRDD {
    msg =>
      val moviesdetails = msg
        .map { v => v.value().split(",") }
        .filter(x => x.contains("m"))
        .map(
          payload => {
            movies(
              payload(0).toInt,
              payload(1),
              payload(2)
            )
          }
        )
        .toDF("movieid", "title", "genres").persist().createOrReplaceTempView("movies")

      val ratingsdetails = msg
        .map { v => v.value().split(",") }
        .filter(x => x.contains("r"))
        .map(
          payload => {
             Ratings(
              payload(0).toInt,
              payload(1).toInt,
              payload(2).toDouble
            )
          }
        ).toDF("userid", "movieid", "rating")

      //joining both historical and live data of ratings
      val uniondataframe =hist_ratingdf.union(ratingsdetails)

      //creating view for the joined ratings data
      uniondataframe.createOrReplaceTempView("ratings")

      //converting uniondataframe to rdd
      val unionRDD=uniondataframe.rdd.map(_.mkString(","))

      //calling recommender object to recommend movies to user
      val recommender = new Recommender(sc,unionRDD,movies_file,rec_userId)

      //==================================METRICS  CALCULATIONS========================================================
      //Count the max, min ratings along with the number of users who have rated a movie
      val max_min_ratings = sqlContext.sql(
      """select movies.movieid,movies.title, movierates.maxrate, movierates.minrate, movierates.usercount
      from(SELECT ratings.movieid, max(ratings.rating) as maxrate, min(ratings.rating) as minrate,
      count(distinct userid) as usercount FROM ratings group by ratings.movieid ) movierates join
      movies on movierates.movieid=movies.movieid order by movierates.usercount desc""")
      max_min_ratings.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "max_min_ratings")).save()

      //Display movie titles according to the year
      val movietitles_based_on_year = sqlContext.sql("SELECT movieid,title FROM movies where  title like '%1993%'")
      movietitles_based_on_year.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "movietitles_based_on_year")).save()

      // Display movie titles according to the alphabets
      val movietitles_with_sorted_order = sqlContext.sql("SELECT movieid,title FROM movies where  title like 'M%' order by title")
      movietitles_with_sorted_order.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "movietitles_with_sorted_order")).save()

      //Count the number of movies rated by a particular user
      val movies_rated_by_specific_user = sqlContext.sql("SELECT ratings.userid,count(*) as numberofmovies FROM movies JOIN ratings on movies.movieid=ratings.movieid where userid='4169' Group By userid")
      movies_rated_by_specific_user.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "movies_rated_by_specific_user")).save()

      // Display the top 10 movies with highest rating given a particular user
      val top10movies = sqlContext.sql("SELECT movies.movieid,movies.title,ratings.rating FROM movies JOIN ratings on movies.movieid=ratings.movieid Order By rating desc limit 10 ")
      top10movies.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "top10movies")).save()

      //show the top 10 most active users and how many times they rated a movie
      val activeusers = sqlContext.sql("""SELECT ratings.userid,count(*) as moviescount from ratings group by ratings.userid order by moviescount desc limit 10""")
      activeusers.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "activeusers")).save()

      // Display the title for movies with ratings > 4
      val movietittles_above_4_ratings = sqlContext.sql("""SELECT movies.movieid, movies.title,ratings.rating FROM ratings JOIN movies ON movies.movieid=ratings.movieid where ratings.rating > 4""")
      movietittles_above_4_ratings.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "movietitles_above_4_ratings")).save()

      //Top rated movie in each genre
      val ratedmovies_base_on_genres =  sqlContext.sql("SELECT movies.movieid, movies.title,ratings.rating,movies.genres FROM movies JOIN ratings on movies.movieid=ratings.movieid Group By movies.movieid,movies.genres,movies.title,ratings.rating  Order By rating desc")
      ratedmovies_base_on_genres.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "ratedmovies_base_on_genres")).save()

      //Which movie is most liked or disliked by the users based on the ratings provided.
      val MostLikedAndDisliked_movies = sqlContext.sql("SELECT ratings.userid,movies.title,ratings.rating as mostlikedanddisliked FROM movies JOIN ratings on movies.movieid=ratings.movieid where ratings.rating > 4 OR ratings.rating<1")
      MostLikedAndDisliked_movies.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "mostlikedanddisliked_movies")).save()

      //Identify the movies not rated by a particular user
      val not_rated_movies = sqlContext.sql("""SELECT ratings.userid,movies.title,ratings.rating FROM ratings JOIN movies ON movies.movieid=ratings.movieid where ratings.rating =0""")
      not_rated_movies.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("keyspace" -> "recommendation", "table" -> "not_rated_movies")).save()
    }
  ssc.start()
  ssc.awaitTermination()

}

