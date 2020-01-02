import com.david.TransactionProcessor
import com.david.TransactionsApp.{df, session}
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

class TransactionProcessorSpec extends WordSpec with Matchers {

  "The TransactionStreamService" should {

    val session = SparkSession.builder().master("local[*]").getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    val df = session.read.option("header",true).csv("src/main/resources/transactions.csv")

    val processor = new TransactionProcessor(df, session)

    "should count the number of transactions" in {
      processor.getNumberTransactions should ===(113801)
    }

    "should find the top N days with more transactions" in {
      processor.topN(3) should ===(List(("27/12/2018",776), ("11/06/2019",771), ("04/05/2019",758)))
    }

    "find the days with zero transactions" in {
      processor.daysZeroTransactions() should === (List("01/07/2019", "02/07/2019", "03/07/2019", "04/07/2019", "05/07/2019", "06/07/2019"))
    }

    "should give the top N hotels" in {
      val topHotels = processor.topHotels(10)
      topHotels should ===(List(("NH",12893), ("Vertice",12719), ("Ibis",12665), ("Holiday Inn",12646),
        ("Hilton",12624), ("Radisson Blue",12602), ("Melia",12577), ("Eurostars",12554), ("Marriot",12521)))
    }

    "should give the top N hotels per month" in {
      val topHotels = processor.topHotelsPerMonth(3)
      val expected = List(("2018/12",List(("Radisson Blue",676), ("Hilton",678), ("Eurostars",690))),
        ("2019/01",List(("Ibis",2106), ("Radisson Blue",2122), ("Hilton",2161))),
        ("2019/02",List(("NH",1833), ("Vertice",1835), ("Holiday Inn",1843))),
        ("2019/03",List(("Eurostars",2005), ("Holiday Inn",2010), ("NH",2057))),
        ("2019/04",List(("Vertice",1955), ("Melia",1980), ("NH",2001))),
        ("2019/05",List(("Ibis",2026), ("Holiday Inn",2035), ("Melia",2044))),
        ("2019/06",List(("Ibis",2014), ("NH",2045), ("Vertice",2087))),
        ("2019/07",List(("Marriot",163), ("Hilton",175), ("NH",179))))
      topHotels should ===(expected)
    }

    "should give the top periods" in {
      val topPeriods = processor.getMostPopularPeriods(3)
      topPeriods should ===(List((("26/08/2019","07/09/2019"),117), (("04/10/2019","05/10/2019"),110), (("30/09/2019","05/10/2019"),110)))
    }

    "should give the average of transactions per hour" in {
      val transactionsPerHour = processor.getAverageTransactionsPerHour()
      transactionsPerHour.take(3) should ===(List(("00",24.768845f), ("01",22.81407f), ("02",23.211056f)))
    }

    "should give transactions per city per month" in {
      val transactionsPerHour = processor.getCityTransactionsPerMonth(6,3)
      transactionsPerHour.take(1) should ===(List(("2019/02",List(("London",2341), ("Paris",2399), ("Budapest",2424)))))
    }
  }
}