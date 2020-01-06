package com.david.dataset
import org.apache.spark.sql.expressions.Window
import com.david.TransactionPerDay
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Days, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

case class InitialTransaction(timestamp: String, transaction_id: String, hotel: String, city: String, from: String, to: String, transaction_name: String)

case class TransactionPerDay(date: String, transactions: Long)


class TransactionDatasetProcessor(df: DataFrame, sparkSession: SparkSession) {
  import org.apache.spark.sql.functions._

  df.createOrReplaceTempView("Transactions")
  import sparkSession.sqlContext.implicits._


  val fmt: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  val initialTransactionsDS= df.as[InitialTransaction]
  val dataframe= df
  val transactionDateDF= initialTransactionsDS.select(from_unixtime(col("timestamp")/1000,"dd/MM/yyyy").as("date"))
  transactionDateDF.cache()
  val groupedByDay=transactionDateDF
    .groupBy("date")
    .count().select($"date",$"count".as("transactions")).as[TransactionPerDay]

  groupedByDay.cache()
  def getNumberTransactions: Long = initialTransactionsDS.count()

  def topN(n: Int): List[(String, Long)] = {
    groupedByDay.orderBy(desc("transactions")).take(n).map(a => (a.date,a.transactions)).toList
  }

  private def getRangeDays() ={
    val minDate = initialTransactionsDS.select(from_unixtime(min(col("timestamp")/1000),"dd/MM/yyyy")).as[String].take(1)(0)
    val maxDate = initialTransactionsDS.select(from_unixtime(max(col("timestamp")/1000),"dd/MM/yyyy")).as[String].take(1)(0)
    val start = LocalDate.parse(minDate, fmt)
    val end = LocalDate.parse(maxDate, fmt)
    val daysCount = Days.daysBetween(start, end).getDays()
    (0 until daysCount).map(start.plusDays(_)).map(l => (l.toString(fmt),0))
  }
  def daysZeroTransactions(): List[String] = {
    val rangeDF= sparkSession.sparkContext.parallelize(getRangeDays()).toDF("date", "transactions").as[TransactionPerDay]
    val result =rangeDF.join(groupedByDay, rangeDF("date") === groupedByDay("date"),"left_anti").as[TransactionPerDay]
    result.collect().map(a => a.date).toList
  }

  def topHotels(n: Int): List[(String, Long)] = {
    val resultDF=initialTransactionsDS.groupBy("hotel").count().orderBy(desc("count"))
    resultDF.take(n).map(r => (r.getString(0), r.getLong(1))).toList
  }

  def topHotelsPerMonth(n: Int): List[(String,List[(String, Long)])] = {
    val groupedHotelDateCount = initialTransactionsDS
      .select(from_unixtime(col("timestamp")/1000,"yyyy/MM").as("monthyear"),$"hotel")
      .groupBy("monthyear","hotel").count()
    val monthYearWindow = Window.partitionBy('monthyear).orderBy('count.desc)
    val rankedHotels = groupedHotelDateCount
      .select($"monthyear", $"hotel", $"count".as("transactions"), dense_rank().over(monthYearWindow).as('rank))
    val resultDF = rankedHotels
      .select('monthyear,'hotel, 'transactions)
      .where('rank <= n)
      .orderBy('monthyear, desc("transactions"))
    val result = resultDF.collect.map(r => (r.getString(0), r.getString(1),r.getLong(2))).toList
    result.groupBy(_._1).mapValues(a => a.map(value =>(value._2,value._3)).sortBy(_._2)).toList.sortBy(_._1)
  }
}
