package com.spark.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class SparkDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(SparkDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		SparkSession spark = SparkSession.builder().appName("Spark SQL examples").master("local").getOrCreate();

		Dataset<Row> data = spark.read().json("/home/anilkumar/git/data.json");

		/*SparkUtil.filterData(data, "gender", "Male");
		SparkUtil.findAll(spark, data);
		SparkUtil.selectColumnsData(data, "fname", "lname");
		SparkUtil.conactColumnsValue(data, "fname", "lname");*/
		SparkUtil.findAll(spark, data);
//		SparkUtil.showSchema(data);
		
//		ParquetExample.parquet(spark, data);
		spark.stop();
	}
	
	
}
