// Start shell by - ./bin/spark-shell --master local[2]  --packages com.databricks:spark-csv_2.10:1.4.0 spark.serializer=org.apache.spark.serializer.KryoSerializer 


val train = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").load("/Users/IronMan/Desktop/av/stream-analytix/training_sample.csv")
// train - shows datatype of train
//train.printSchema
// train.show // list top 10 rows of a dataframe
val duration_value =train.select("duration").groupBy("duration").agg(count("duration").as("ct")).orderBy($"ct".desc)
val src_bytes_value =train.select("src_bytes").groupBy("src_bytes").agg(count("src_bytes").as("ct")).orderBy($"ct".desc)
val dst_bytes_value =train.select("dst_bytes").groupBy("dst_bytes").agg(count("dst_bytes").as("ct")).orderBy($"ct".desc)
val rel_dur_dest = train.select("duration","dst_bytes").groupBy("duration","dst_bytes").agg(count("dst_bytes").as("ct")).orderBy($"ct".desc

val rel_dur_src = train.select("duration","src_bytes").groupBy("duration","src_bytes").agg(count("src_bytes").as("ct")).orderBy($"ct".desc)
val rel_dur_src_dest = train.select("duration","src_bytes","dst_bytes").groupBy("duration","src_bytes","dst_bytes").agg(count("dst_bytes").as("ct")).orderBy($"ct".desc)
val land_values = train.select("land").groupBy("land").agg(count("land").as("ct")).orderBy($"ct".desc) // Seems variables has most values 0 and thus could be zero information variable
val root_shell_values = train.select("root_shell").groupBy("root_shell").agg(count("root_shell").as("ct")).orderBy($"ct".desc)

train.select("root_shell","su_attempted","num_root","num_access_files","is_host_login","is_guest_login").where("root_shell=1").show // seems values with rrot_shell doesn;t change much... but when they change... there can be a problem

train.where("protocol_type_tcp=1 and protocol_type_icmp =1").show // no row so that means protocol can be any one.. so clear distinction.. Also both can be zero

train.where("service_ecr_i=1 and service_private=1").show // no row so that means protocol can be any one.. so clear distinction.. Also both can be zero

train.where("flag_S0=1 and flag_SF=1").show // no row so that means protocol can be any one.. so clear distinction.. Also both can be zero

val count_values = train.select("count").groupBy("count").agg(count("count").as("ct")).orderBy($"ct".desc) // count_values.count - 377 different values

val srv_count_values = train.select("srv_count").groupBy("srv_count").agg(count("srv_count").as("ct")).orderBy($"ct".desc) //srv_count_values.count - 227

 val guest_values = train.select("is_guest_login").groupBy("is_guest_login").agg(count("is_guest_login").as("ct")).orderBy($"ct".desc) //  Only 16 guest login .. need to see their charecteristics  0|20025|| 1|   16|

val is_host_values = train.select("is_host_login").groupBy("is_host_login").agg(count("is_host_login").as("ct")).orderBy($"ct".desc) // 

/* is_host_values.show
+-------------+-----+
|is_host_login|   ct|
+-------------+-----+
|            0|20041|
+-------------+-----+
Only one value - so doesnt matter


*/

train.where("is_host_login = 0 and is_guest_login = 0").count // 20025
train.where("is_host_login = 0 and is_guest_login = 1").count // 16 
// Not much information in this coz is_host_login is independent of is_guest_login


// ref- https://spark.apache.org/docs/latest/mllib-statistics.html
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg._

// To convert to vector
val ct = train.rdd.map(row => {
      Vectors.dense(row.toSeq.toArray.map({
      	case s: Double => s
      	case l: Integer => l.toDouble
      	case d: String => 0.0
      }))})

// Compute column summary statistics.
val summary: MultivariateStatisticalSummary = Statistics.colStats(ct)
println(summary.mean)  // a dense vector containing the mean value for each column
println(summary.variance)  // column-wise variance
println(summary.numNonzeros)  // number of nonzeros in each column

val correlMatrix: Matrix = Statistics.corr(ct, "pearson")
println(correlMatrix.toString)

// Similarly you can try diiferent measures tu understand data //
