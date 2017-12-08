/**
  * creating case classes for movies and historical ratings dataset
  */
package com.dfz.re.Model

case class movies (
    movieid: Int,
    title: String,
    genres: String
                  )

case class Ratings (
    userId: Int,
    movieId: Int,
    rating: Double
                   )
