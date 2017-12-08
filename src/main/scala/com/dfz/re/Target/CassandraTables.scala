/**
  * Creating cassandra Keyspace and tables to store KafkaConsumerSpark
  * metrics results into the particular Cassandra table
  */
package com.dfz.re.Target

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object CassandraTables {

  def main(args: Array[String]) {

    val appName="CassandraTable"

    //Initialising Spark Context
    val conf=new SparkConf()
            .setAppName(appName)
            .setMaster("local[*]")
            .set("spark.cassandra.connection.host", args(0).toString)//"10.1.51.42"
    val sc=new SparkContext(conf)

    //Initialising Cassandra Connector
    val cassandraConnector = CassandraConnector.apply(conf)

    cassandraConnector.withSessionDo(session => {
      //Creating Keyspace recommendation in Cassandra
      session.execute(s"""DROP KEYSPACE IF EXISTS recommendation;""")
      session.execute("CREATE KEYSPACE IF NOT EXISTS recommendation WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} ")

      // Creating table to Count the max, min ratings along with the number of users who have rated a movie.
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.max_min_ratings (
                          movieid int PRIMARY KEY,
                          title text,
                          maxrate float,
                          minrate float,
                          usercount int
                          )
                          ;""")
      //Creating table to get top N recommendations
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.top_n_recommendations (
                          userid int ,
                          title text PRIMARY KEY,
                          rating float
                          )
                          ;""")
      //Creating table to display the title for movies with ratings > 4
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.movietitles_above_4_ratings (
                          movieid int PRIMARY KEY,
                          title text,
                          rating float
                          )
                          ;""")
      //Creating table to display movie titles according to the year
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.movietitles_based_on_year (
                          movieid int PRIMARY KEY,
                          title text
                          )
                          ;""")
      //Creating table to display movie titles according to the alphabets
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.movietitles_with_sorted_order (
                          movieid int PRIMARY KEY,
                          title text
                          )
                          ;""")
      //Creating table to Count the number of movies rated by a particular user
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.movies_rated_by_specific_user (
                          userid int PRIMARY KEY,
                          numberofmovies int
                          )
                          ;""")
      //Creating table to show the top 10 most active users and how many times they rated a movie
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.activeusers (
                          userid int PRIMARY KEY,
                          moviescount int
                          )
                          ;""")
      //Creating table to display the top 10 movies with highest rating given a particular user
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.top10movies (
                          movieid int PRIMARY KEY,
                          title text,
                          rating float
                          )
                          ;""")
      //Creating table to display which movie is most liked or disliked by the users based on the ratings provided.
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.mostlikedanddisliked_movies (
                          userid int PRIMARY KEY,
                          title text,
                          mostlikedanddisliked float
                          )
                          ;""")
      //Creating table to display top rated movie in each genre
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.ratedmovies_base_on_genres (
                          movieid int PRIMARY KEY,
                          title text,
                          rating float,
                          genres text,
                          )
                          ;""")
      //Identify the movies not rated by a particular user
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.not_rated_movies(
                          userid int PRIMARY KEY,
                          title text,
                          rating float
                          )
                          ;""")
      //Identify the movies not rated by a particular user
      session.execute(s"""CREATE TABLE IF NOT EXISTS recommendation.predict_ratings(
                          rating float PRIMARY KEY
                          )
                          ;""")
      //Truncating the records
      session.execute(s"""TRUNCATE TABLE recommendation.max_min_ratings;""")
      session.execute(s"""TRUNCATE TABLE recommendation.top_n_recommendations""")
      session.execute(s"""TRUNCATE TABLE recommendation.movietitles_above_4_ratings""")
      session.execute(s"""TRUNCATE TABLE recommendation.movietitles_based_on_year""")
      session.execute(s"""TRUNCATE TABLE recommendation.movietitles_with_sorted_order""")
      session.execute(s"""TRUNCATE TABLE recommendation.movies_rated_by_specific_user""")
      session.execute(s"""TRUNCATE TABLE recommendation.activeusers""")
      session.execute(s"""TRUNCATE TABLE recommendation.top10movies""")
      session.execute(s"""TRUNCATE TABLE recommendation.MostLikedAndDisliked_movies""")
      session.execute(s"""TRUNCATE TABLE recommendation.ratedmovies_base_on_genres""")
      }
    )
    System.out.println("==================================================================================")

    sc.stop()
  }

}
