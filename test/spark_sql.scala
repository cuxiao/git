/**
 * Using Spark SQL to calcualte the Opt Point of each transaction
 */

val df = sqlContext.read.load("/user/jiaqizhang/spark/examples/ato_scores.parquet")

val test_table = df.select("trans_id", "ATOM14_NN_SCORE", "unit_wgt", "ato_bad_1").sort($"ATOM14_NN_SCORE".desc)

test_table.registerTempTable("test_table")


val result = sqlContext.sql("select trans_id, ATOM14_NN_SCORE, unit_wgt, ato_bad_1, sum(unit_wgt ) over ( order by ATOM14_NN_SCORE desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) as cumelate_sum, sum(unit_wgt) over (order by ATOM14_NN_SCORE desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total,  1000 * (sum(unit_wgt) over ( order by ATOM14_NN_SCORE desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )) / sum(unit_wgt) over (order by ATOM14_NN_SCORE  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as opt from test_table order by ATOM14_NN_SCORE desc ").cache

import org.apache.spark.sql.functions._

val toInt    = udf[Int, Double]( num => Math.ceil(num).toInt)
val scoreFormat   = udf((t: Double) => "%.4f".format(t) )

val result2 = result.withColumn("ATOM14_NN_SCORE_f", scoreFormat(result("ATOM14_NN_SCORE"))).withColumn("opt_int", toInt(result("opt"))).select("trans_id", "ATOM14_NN_SCORE", "ATOM14_NN_SCORE_f", "opt_int", "opt", "ato_bad_1", "unit_wgt")

result2.registerTempTable("test_result")

val groupOpt = sqlContext.sql("select opt_int, min(ATOM14_NN_SCORE) as from_score, max(ATOM14_NN_SCORE) as to_score, sum(unit_wgt) as bin_count, sum(CASE when ato_bad_1 == 0 then unit_wgt else 0 end) as bin_good, sum(CASE when ato_bad_1 == 1 then unit_wgt else 0 end) as bin_bad from test_result group by opt_int order by opt_int").cache

groupOpt.registerTempTable("groupOpt")

val optCatch = sqlContext.sql("select opt_int, from_score, to_score, bin_count, bin_good, bin_bad, (bin_count-bin_bad-bin_good) as bin_ind, sum(bin_bad) over ( order by opt_int asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) as cume_bad, sum(bin_bad) over ( order by opt_int asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) / sum(bin_bad) over ( order by opt_int asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) as bad_catch_rate from groupOpt")

optCatch.write.mode("overwrite").parquet("/user/jiaqizhang/spark/examples/ato_gainchart.parquet")