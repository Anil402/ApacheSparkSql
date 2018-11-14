package com.spark.demo;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetExample {

	public static void parquet(SparkSession spark, Dataset<Row> peopleDF) {

		peopleDF.write().parquet("people.parquet");

		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT lname FROM parquetFile WHERE id BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map(new MapFunction<Row, String>() {
			public String call(Row row) {
				return "Name: " + row.getString(1);
			}
		}, Encoders.STRING());
		namesDS.show();
	}
}
