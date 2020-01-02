# Format
 timestamp, transactionId, Hotel Name, City, from, to, Name
 1562672040000,279,Holiday Inn,Tokio,02/09/2019,09/09/2019,Cassius Montes

 There is a CSV file that contains a sequence of hotel transactions.
 The transactions contain a column timestamp, that is the moment that the transaction was performed.
 Note that there are some gaps in the data. There are some days without transactions.

 # Stage 1
   * Give me the total number of transactions.
   * Give me top N days with the most transactions. The information should contain the day and the number of transactions.
   * Find days with zero transactions.
   * Give the the N most popular hotels.

 # Stage 2
   * Return the report with the relationship city - transactions per month in the last N months and K cities.
   * Give the the most popular N periods. A period correspond to the fields from and to.
   * Give me the most popular hotel per month.
   * Give me the average of transactions per hour.
