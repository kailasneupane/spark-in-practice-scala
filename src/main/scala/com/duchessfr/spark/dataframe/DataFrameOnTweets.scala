package com.duchessfr.spark.dataframe

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
 *  The Spark SQL and DataFrame documentation is available on:
 *  https://spark.apache.org/docs/1.4.0/sql-programming-guide.html
 *
 *  A DataFrame is a distributed collection of data organized into named columns.
 *  The entry point before to use the DataFrame is the SQLContext class (from Spark SQL).
 *  With a SQLContext, you can create DataFrames from:
 *  - an existing RDD
 *  - a Hive table
 *  - data sources...
 *
 *  In the exercise we will create a dataframe with the content of a JSON file.
 *
 *  We want to:
 *  - print the dataframe
 *  - print the schema of the dataframe
 *  - find people who are located in Paris
 *  - find the user who tweets the more
 * 
 *  And just to recap we use a dataset with 8198 tweets,where a tweet looks like that:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 * 
 *  Use the DataFrameOnTweetsSpec to implement the code.
 */
object DataFrameOnTweets {


  val pathToFile = "data/reduced-tweets.json"

  val spark = SparkSession
    .builder()
    .config("spark.master", "local[*]")
    .appName("Dataframe")
    .getOrCreate()

  import spark.implicits._

  /**
   *  Here the method to create the contexts (Spark and SQL) and
   *  then create the dataframe.
   *
   *  Run the test to see how looks the dataframe!
   */
  def loadData(): DataFrame = {
    // create spark configuration and spark context
    /*
    val conf = new SparkConf()
        .setAppName("Dataframe")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)
    */

    // Create a sql context: the SQLContext wraps the SparkContext, and is specific to Spark SQL.
    // It is the entry point in Spark SQL.
    // TODO write code here

    /**
      * above mentioned technique is deprecated now
      * new code is mentioned above in global scope
      */

    // Load the data regarding the file is a json file
    // Hint: use the sqlContext and apply the read method before loading the json file
    // TODO write code here
    // above mentioned technique is deprecated now

    val sqlcontext: DataFrame = spark.read.json(pathToFile)
    //var display:Unit = sqlcontext.show(sqlcontext.count().toInt)
    sqlcontext
  }


  /**
   *  See how looks the dataframe
   */
  def showDataFrame() = {
    val dataframe = loadData()

    // Displays the content of the DataFrame to stdout
    // TODO write code here
    dataframe.show() //shows top 20 rows only by default
  }

  /**
   * Print the schema
   */
  def printSchema() = {
    val dataframe = loadData()

    // Print the schema
    // TODO write code here
    dataframe.printSchema()
  }

  /**
   * Find people who are located in Paris
   */
  def filterByLocation(): DataFrame = {
    val dataframe = loadData()

    // Select all the persons which are located in Paris
    // TODO write code here
    //by using sql
    dataframe.createOrReplaceTempView("tweets_info") //creating table name
    var sqlDF = spark.sql("select user,place from tweets_info where place = \"Paris\"")
    sqlDF.show()
    println("data count sqlDF: " + sqlDF.count())

    //using dataframe
    var ddf = dataframe.filter($"place".equalTo("Paris"))
      .select("user", "place")
    ddf.show()
    println("data count ddf: " + ddf.count())

    ddf
  }


  /**
   *  Find the user who tweets the more
   */
  def mostPopularTwitterer(): (Long, String) = {
    val dataframe = loadData().persist(StorageLevel.MEMORY_ONLY)

    // First group the tweets by user
    // Then sort by descending order and take the first one
    // TODO write code here

    //using sql query
    dataframe.createOrReplaceTempView("tweets_info")
    var sqlDF = spark.sql("select user from tweets_info group by user order by count(user) DESC")
    var sqlDF1 = spark.sql("select count(user) from tweets_info group by user order by count(user) DESC")
    sqlDF.show()
    var who_tweets_more = sqlDF.first().getString(0)
    var no_of_tweets = sqlDF1.first().getLong(0)
    println("who tweets the more: " + who_tweets_more)
    println("total count of tweeters: " + no_of_tweets)
    //(no_of_tweets, who_tweets_more)

    //using dataframe / rdd operations
    var df: Row = dataframe.groupBy("user")
      .count()
      .rdd.sortBy(x => x.getLong(1), false).first()
    println("first row: " + df)
    dataframe.unpersist()
    (df.getLong(1), df.getString(0))
  }

}
