import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

import java.util.Arrays;

public class Task1 {

	static final String dataFilePath = "../data/ALLvideos.csv";

	static final String firstCountry = "GB";
	static final String secondCountry = "DE";

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Task1").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

		Dataset<Row> df = spark.read().format("csv") 
			.option("multiLine", "true")
			.option("header", "true")
			.load(dataFilePath);

		Dataset<Row> r1 = df.filter(col("country").equalTo(firstCountry))
			.select("video_id", "category")
			.distinct()
			.groupBy("category")
			.count()
			.withColumnRenamed("count", firstCountry + "-total");

		
		Dataset<Row> df1 = df.filter(col("country").equalTo(firstCountry)).select("video_id", "category").distinct();
		Dataset<Row> df2 = df.filter(col("country").equalTo(secondCountry)).select("video_id", "category").distinct();

		Dataset<Row> r2 = df1.intersect(df2)
			.groupBy("category").count()
			.withColumnRenamed("count", "common-in-" + secondCountry);

		Dataset<Row> results = r1.join(r2, r1.col("category").equalTo(r2.col("category")), "left")
			.select(r1.col("category"), r1.col(firstCountry + "-total"), r2.col("common-in-" + secondCountry))
			.na().fill(0);
		

		results.show();
		

		results.javaRDD()
			.map(row -> row.get(0) + "; total: " + row.get(1) + "; " + String.format("%.1f", (Double.valueOf(row.get(2).toString()) * 100 / Double.valueOf(row.get(1).toString()))) + "% in " + secondCountry)
			.repartition(1).saveAsTextFile("output");
		spark.stop();
	}
}