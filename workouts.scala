package org.inceptez.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import sparkSession.sqlContext.implicits._


case class insureclass(IssuerId:String,IssuerId2:String,BusinessDate:String,StateCode:String,SourceName:String,NetworkName:String,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)
/*case class insureclass1(IssuerId:String,IssuerId2:String,BusinessDate:String,StateCode:String,SourceName:String,NetworkName:String,NetworkURL:String,custnum:String,MarketCoverage:String,DentalOnlyPlan:String)*/

object workouts {
def main (args:Array[String])
{
 val conf = new  SparkConf().setAppName("spark").setMaster("Local[*]")
 val outDirPath = "hdfs://localhost:54310/user/hduser/sparkhack2/outputpath/"
 val sc   = new SparkContext(conf)
 val sqlc = new SQLContext(sc);
 
 println("---> SPARK HACKATHON 2019 <-----");
 
 val insuredata = sc.textFile("hdfs:///user/hduser/sparkhack2/insuranceinfo1.csv");
 println("No of lines before removing header from insuranceinfo1.csv " +insuredata.count);
 
 val header = insuredata.first();
 val insuredata_noheader = insuredata.filter(row => row !=header);

 println("No of lines after removing header from insuranceinfo1.csv " +insuredata_noheader.count);
 
 val insuredata_split = insuredata_noheader.map(x=> x.split(",")).filter(y=> y.length>0); 
 
 val insuredata_10field = insuredata_noheader.map(x=> x.split(",")).filter(y=> y.length==10);

 val insuredata_schemardd = insuredata_10field.map(x=> insureclass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)));

 println("Step7 insuredata_schemardd.count "+insuredata_schemardd.count);
 println("step1 insuredata.count "+insuredata.count);
 
 
 val rejectdata = insuredata_noheader.map(x=> x.split(",")).filter(y=> y.length!=10)
 
 println("These no's of data lines gets rejected due to Marketcoverage and DentalOnlyPlan contains no data "+rejectdata);
 
 /*****insuredata 2 *****/

 val insuredata2 = sc.textFile("hdfs:///user/hduser/sparkhack2/insuranceinfo2.csv");
 
 val header2 = insuredata2.first();
 val insuredata2_noheader = insuredata2.filter(row => row != header2);

 println("No of lines after removing header from insuranceinfo1.csv " +insuredata_noheader.count);
 
 val insuredata2_split = insuredata2_noheader.map(x=> x.split(",")).filter(y=> y.length>0); 
 
  /*1822*/
 val insuredata2_10field = insuredata2_noheader.map(x=> x.split(",")).filter(y=> y.length==10); 

 val insuredata2_schemardd = insuredata2_10field.map(x=> insureclass(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)));

 println("Step7 insuredata_schemardd.count "+insuredata2_schemardd.count);
 println("step1 insuredata.count "+insuredata2.count);
 
 /*1511*/
 val rejectdata2 = insuredata2_noheader.map(x=> x.split(",")).filter(y=> y.length!=10)
 
 println("These no's of data lines gets rejected due to Marketcoverage and DentalOnlyPlan contains no data "+rejectdata2);

 /****Section 2*****/
 val  insuredatamerged = insuredata_schemardd.union(insuredata2_schemardd);
 
 import org.apache.spark.storage._
 insuredatamerged.persist(StorageLevel.MEMORY_AND_DISK_2);
 
 println("Top 5 records "+insuredatamerged.take(5));
 
 
 val totaldata = insuredata2_schemardd.count + rejectdata2.count 

 println(" total count "+totaldata);
 
 /*15. Remove duplicates from this merged RDD created in step 12 */
 
 val uniquerdd = insuredatamerged.distinct;
 
 val distinctcount = insuredatamerged.distinct.count;
 val mergedatacount = insuredatamerged.count 

 val duplicaterows = mergedatacount - distinctcount; 

 println("no of duplicate rows "+duplicaterows);
 
 /*16. Increase the number of partitions to 8 and name it as insuredatarepart.*/
 
 val insuredatarepart = uniquerdd.repartition(8)
 println(" increased num of partitions "+insuredatarepart.getNumPartitions);
 
 /*17. Split the above RDD using the businessdate field into rdd_20191001 and rdd_ 20191002 based 
  * on the BusinessDate of 2019-10-01 and 2019-10-02 respectively*/

 val businessdate_20191001 = insuredatarepart.filter(x=> x.BusinessDate.contains("2019-10-01"))
 val businessdate_20191002 = insuredatarepart.filter(x=> x.BusinessDate.contains("2019-10-02"))
 
/*18. Store the RDDs created in step 9, 12, 17 into HDFS locations.*/ 
rejectdata.saveAsTextFile("hdfs://localhost:54310//user/hduser/sparkhack2/rejectdata");
rejectdata2.saveAsTextFile("hdfs://localhost:54310//user/hduser/sparkhack2/rejectdata2");
insuredatamerged.saveAsTextFile("hdfs://localhost:54310//user/hduser/sparkhack2/insuredatamerged");
businessdate_20191001.saveAsTextFile("hdfs://localhost:54310//user/hduser/sparkhack2/businessdate_20191001");
businessdate_20191002.saveAsTextFile("hdfs://localhost:54310//user/hduser/sparkhack2/businessdate_20191002");

/* 19. Convert the RDD created in step 16 above into Dataframe namely insuredaterepartdf applying the structtype 
 * created in the step 20 given in the next usecase. */
import sqlc.implicits._
val insuredaterepartdf = insuredatarepart.toDF();


println("creating dataframe using createdataframe applying structure type")
    
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder().appName("IZ Test app").master("local[*]").getOrCreate();

 val structypeschema = StructType(Array(
        StructField("IssuerId",       IntegerType, true),
        StructField("IssuerId2",      IntegerType, true),
        StructField("BusinessDate",   DateType, true),
        StructField("StateCode",      StringType, true),
        StructField("SourceName",     StringType, true),
        StructField("NetworkName",    StringType, true),
        StructField("NetworkURL",     StringType, true),
        StructField("custnum",        StringType, true),        
        StructField("MarketCoverage", StringType, true),
        StructField("DentalOnlyPlan", StringType, true)
        ))
val insuranceinfo1 = sqlc.read.option("delimiter",",").option("header","true").option("escape",",").schema(structypeschema).csv("hdfs:///user/hduser/sparkhack2/insuranceinfo1.csv")    
val insuranceinfo2 = sqlc.read.option("delimiter",",").option("header","true").option("escape",",").schema(structypeschema).csv("hdfs:///user/hduser/sparkhack2/insuranceinfo2.csv")    

insuranceinfo1.show(10);
insuranceinfo2.show(10);

/*21. Apply the below DSL functions in the DFs created in step 21.*/
val insuranceinfo1dsl = insuranceinfo1.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm").withColumn("issueridcomposite",concat(col("IssuerId"),col("IssuerId2"))).drop("DentalOnlyPlan").withColumn("sysdatetime",lit(current_timestamp()));
val insuranceinfo2dsl = insuranceinfo2.withColumnRenamed("StateCode", "stcd").withColumnRenamed("SourceName", "srcnm").withColumn("issueridcomposite",concat(col("IssuerId"),col("IssuerId2"))).drop("DentalOnlyPlan").withColumn("sysdatetime",lit(current_timestamp()));

/* insuranceinfo1dsl.count -> 401
 * insuranceinfo2dsl.count -> 3333
 */
 /*22. Remove the rows contains null in any one */
 val insuranceinfo1_any = insuranceinfo1dsl.na.drop("any");
 val insuranceinfo2_any = insuranceinfo2dsl.na.drop("any");
 
def remspecialchar(String : String ): String = {
String.replaceAll("[^a-zA-B0-9]"," ")  
}
/*26*/
val dsludf = udf(remspecialchar(_: String): String)
val insuranceinfo1_final = insuranceinfo1_any.withColumn("NetworkName",dsludf(col("NetworkName")));
val insuranceinfo2_final = insuranceinfo2_any.withColumn("NetworkName",dsludf(col("NetworkName")));


/*
// 27
insuranceinfo1_final.write.format("json").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo1JSON")
insuranceinfo2_final.write.format("json").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo2JSON")
//Point - 28
insuranceinfo1_final.write.format("csv").option("header", "true").option("delimiter", "~").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo1CSV")
insuranceinfo2_final.write.format("csv").option("header", "true").option("delimiter", "~").mode(SaveMode.Overwrite).save(outDirPath + "processedInfo2CSV")
*/

/*29*/
 val custstates = sc.textFile("hdfs:///user/hduser/sparkhack2/custs_states.csv");
/*30*/  
 val custfilter = custstates.map(x=> x.split(",")).filter(y=> y.length == 5);
 val statefilter = custstates.map(x=> x.split(",")).filter(y=> y.length == 2);

/*31*/
val custstatesdf = spark.read.option("delimiter", ",").option("escape", ",").csv("hdfs:///user/hduser/sparkhack2/custs_states.csv").toDF("Col1", "Col2", "Col3", "Col4", "Col5")
/*32*/
val custfilterdf = custstatesdf.na.drop(Array("Col1","Col2","Col3", "Col4", "Col5")).withColumnRenamed("Col1", "custid").withColumnRenamed("Col2", "FirstName").withColumnRenamed("Col3", "LastName").withColumnRenamed("Col4", "age").withColumnRenamed("Col5", "profession")
val statefilterdf = custstatesdf.filter("Col3 is null and Col4 is null and Col5 is null").drop("Col3","Col4","Col5").withColumnRenamed("Col1","state").withColumnRenamed("Col2","statedesc")
/*33*/
custfilterdf.createOrReplaceTempView("custview")
statefilterdf.createOrReplaceTempView("statesview")
/*34*/
insuranceinfo1_any.createOrReplaceTempView("insuranceinfo1view")
insuranceinfo2_any.createOrReplaceTempView("insuranceinfo2view")
/*35*/
spark.sqlContext.udf.register("removeSpecialCharacter", remspecialchar(_: String): String)
/*36*/
val finalinsuranceinfo1df = spark.sql("select insuranceinfo1view.*,removeSpecialCharacter(NetworkName) as cleannetworkname,current_date() as curdt,current_timestamp() as curts,year(BusinessDate) as yr,month(BusinessDate) as mth,case when locate('http' , NetworkURL) > 0 THEN 'http' else 'noprotocol' end as protocol,statesview.statedesc,custview.age,custview.profession from insuranceinfo1view  inner join statesview on insuranceinfo1view.stcd = statesview.stated inner join custView on insuranceinfo1view.custnum = custView.custid  ")
val finalinsuranceInfo2DF = spark.sql("select insureInfo2View.*,removeSpecialCharacter(NetworkName) as cleannetworkname,current_date() as curdt,current_timestamp() as curts,year(BusinessDate) as yr,month(BusinessDate) as mth,case when locate('http' , NetworkURL) > 0 THEN 'http' else 'noprotocol' end as protocol,statesview.statedesc,custview.age,custview.profession from insureInfo2View  inner join statesView on insureInfo2View.stcd = statesView.stated inner join custView on insureInfo2View.custnum = custView.custid  ")


}
}