package Jobs

import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.functions._
object Master1 extends App {
  val Job = "Job_master"



  val spark = SparkSession
    .builder()
    .appName(Job)
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val dataSeq = Seq(("Java", 20000), ("Python", 100000),
    ("Scala", 3000))
  val rdd = spark.sparkContext.parallelize(dataSeq)
  println("Nombre initial de partitions :" +
    rdd.getNumPartitions)
  println("Collect :" + rdd.collect())
  println("Premier element :" + rdd.first())




  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"),
    ("Scala", "3000"))
  val rdd1 = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd1.toDF()
  dfFromRDD1.printSchema()

}



// ./spark-submit --class Jobs.Master1 --master local /home/abraham/IdeaProjects/tp_hadoop/target/scala-2.12/tp_hadoop_2.12-0.1.0-SNAPSHOT.jar


