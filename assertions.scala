import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.DataFrameStatFunctions



 object mariosimplicit{ 
  implicit class DataFrameassertions(df : DataFrame){
		
 implicit def at_most( n : Int, expression : String) : DataFrame = {
	val count = df.filter(expression).count
	 if(count > n) {
 		 throw new Exception(s"Assertion error found, $count found")
	 } else{
		 println(s"Success, $count found" )
		 df
		//.agg(collect_list(struct($"name", $"lat")))
		//.rdd.saveAsTextFile(path)
	   //df.filter(expression)
		
	 }
 }
 
 def at_least(n : Int, expression : String) : DataFrame = {
	 val count = df.filter(expression).count
	 if(count < n) {
		 throw new Exception(s"Assertion error found, $count found")
	 } else{
		 println(s"Success, $count found" )
		 df
		 //.agg(collect_list(struct($"name", $"lat")))
		 //.rdd.saveAsTextFile(path)
		 //val df1 = df.filter(expression)
	 }
 }
 
 def mean_s(col_name: String, start : Double, end : Double) : DataFrame = {
	 val mvalue = df.select(mean(df(col_name))).head().getDouble(0)
	 if (mvalue >= start && mvalue <= end) {
	 println(mvalue)
	 df	
 	 } else {
	 throw new Exception(s"Assertion error found, $mvalue found")
	 //df
 }
 }
 
 def stddev_s(col_name : String, start : Double, end : Double) : DataFrame = {
	 val stddev_val = df.select(stddev(df(col_name))).head().getDouble(0)
	 if (stddev_val >= start && stddev_val <= end){
		 println(stddev_val)
		 df
	 } else {
	 	throw new Exception(s"Assertion error found, $stddev_val found")
	 }
 }
 
 def cov_s(col_name1 : String, col_name2 : String, start : Double, end : Double) : DataFrame = {
	 val cov_val = df.select(covar_pop(col_name1,col_name2)).head().getDouble(0)
	 if (cov_val >= start && cov_val <= end){
		 println(cov_val)
		 df
	 } else {
	 	throw new Exception(s"Assertion error found, $cov_val found")
	 }
 }
 
 def corr_s(col_name1 : String, col_name2 : String, start : Double, end : Double) : DataFrame = {
	 val cor_val = df.select(corr(col_name1,col_name2)).head().getDouble(0)
	 if (cor_val >= start && cor_val <= end){
		 println(cor_val)
		 df
	 } else {
	 	throw new Exception(s"Assertion error found, $cor_val found")
	 }
 }

}
}	