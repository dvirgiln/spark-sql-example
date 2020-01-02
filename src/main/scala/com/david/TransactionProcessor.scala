package com.david

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.joda.time.format.DateTimeFormatter
import org.joda.time.{Days, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

case class Transaction(date: String, transaction_id: Long, hotel: String, city: String, from: String, to: String, transaction_name: String)

case class TransactionPerDay(date: String, transactions: Long)


class TransactionProcessor(df: DataFrame, sparkSession: SparkSession) {
  import org.apache.spark.sql.functions._

  df.createOrReplaceTempView("Transactions")
  import sparkSession.sqlContext.implicits._


  val fmt: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  val groupedDF = sparkSession.sql("SELECT date, count(*) as transactions from (SELECT from_unixtime(timestamp/1000,'dd/MM/yyyy') as date  from Transactions) group by date")
  val groupedByDay = groupedDF.as[TransactionPerDay]

  groupedByDay.cache()
  def getNumberTransactions: Long = df.count()

  def topN(n: Int): List[(String, Long)] = {
    groupedByDay.orderBy(desc("transactions")).take(n).map(a => (a.date,a.transactions)).toList
  }

  private def getRangeDays() ={
    val min = sparkSession.sql("SELECT from_unixtime(min(timestamp)/1000,'dd/MM/yyyy') from Transactions").as[String].take(1)(0)
    val max = sparkSession.sql("SELECT from_unixtime(max(timestamp)/1000,'dd/MM/yyyy') from Transactions").as[String].take(1)(0)
    val start = LocalDate.parse(min, fmt)
    val end = LocalDate.parse(max, fmt)
    val daysCount = Days.daysBetween(start, end).getDays()
    (0 until daysCount).map(start.plusDays(_)).map(l => (l.toString(fmt),0))
  }
  def daysZeroTransactions(): List[String] = {
    val rangeDF= sparkSession.sparkContext.parallelize(getRangeDays()).toDF("date", "transactions").as[TransactionPerDay]
    val result =rangeDF.join(groupedByDay, rangeDF("date") === groupedByDay("date"),"left_anti").as[TransactionPerDay]
    result.collect().map(a => a.date).toList
  }

  def topHotels(n: Int): List[(String, Long)] = {
    val groupedDF = sparkSession.sql("SELECT hotel, count(*) as transactions  from Transactions group by hotel order by transactions desc")
    groupedDF.take(n).map(r => (r.getString(0), r.getLong(1))).toList
  }


  def topHotelsPerMonth(n: Int): List[(String,List[(String, Long)])] = {
    val groupedDF = sparkSession.sql("SELECT monthyear, hotel, transactions from " +
      "(SELECT monthyear, hotel, transactions,dense_rank() OVER (PARTITION BY monthyear ORDER BY transactions DESC) as rank FROM " +
      "(SELECT from_unixtime(timestamp/1000,'yyyy/MM') AS monthyear, hotel, count(*) as transactions FROM Transactions group by monthyear, hotel))" +
      s" WHERE rank <= $n order by monthyear ASC, transactions DESC")
    val result = groupedDF.collect.map(r => (r.getString(0), r.getString(1),r.getLong(2))).toList

    result.groupBy(_._1).mapValues(a => a.map(value =>(value._2,value._3)).sortBy(_._2)).toList.sortBy(_._1)
  }


  def getCityTransactionsPerMonth(months: Int, cities: Int): List[(String,List[(String,Long)])] = {
    val monthsDF =sparkSession.sql(s"SELECT from_unixtime(timestamp/1000,'yyyy/MM') AS monthyear FROM Transactions group by monthyear order by monthyear desc limit $months")
    monthsDF.createOrReplaceTempView("MONTHS_YEAR")

    val transactionsPerCityDF= sparkSession.sql("SELECT from_unixtime(timestamp/1000,'yyyy/MM') AS monthyear, city, count(*) as transactions FROM Transactions group by monthyear, city")
    transactionsPerCityDF.createOrReplaceTempView("TRANSACTIONS_PER_CITY")
    val transactionsDf=sparkSession.sql(s"SELECT monthyear, city, transactions from (SELECT monthyear, city, transactions, dense_rank() OVER (PARTITION BY monthyear ORDER BY transactions DESC) as rank FROM " +
      s"(SELECT TRANSACTIONS_PER_CITY.monthyear, TRANSACTIONS_PER_CITY.city, TRANSACTIONS_PER_CITY.transactions FROM TRANSACTIONS_PER_CITY " +
      s"inner join MONTHS_YEAR on MONTHS_YEAR.monthyear = TRANSACTIONS_PER_CITY.monthyear)) " +
      s"where rank <= $cities")
    val result = transactionsDf.collect.map(r => (r.getString(0), r.getString(1), r.getLong(2))).toList
    result.groupBy(_._1).mapValues(_.map(a => (a._2,a._3)).sortBy(_._2)).toList.sortBy(_._1)
  }

  def getAverageTransactionsPerHour(): List[(String,Float)] = {
    val range = getRangeDays()
    val days = range.size
    val transactionsDF = sparkSession.sql("SELECT hour, count(*) as transactions from (SELECT from_unixtime(timestamp/1000,'HH') as hour  from Transactions) group by hour order by hour")

    transactionsDF.collect().map(r => (r.getString(0), (r.getLong(1).toFloat/days.toFloat))).toList
  }


  def getMostPopularPeriods(n: Int): List[((String,String),Long)] = {
    val outputDF =sparkSession.sql("SELECT from, to, count(*)  as transactions  from Transactions group by from, to order by transactions desc")

    outputDF.take(n).map(r => (((r.getString(0),r.getString(1)),r.getLong(2)))).toList
  }


}
