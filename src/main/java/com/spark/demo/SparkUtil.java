package com.spark.demo;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;

public class SparkUtil {

	public static void findAll(SparkSession spark, Dataset<Row> data) {
		data.createOrReplaceTempView("people");
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
	}

	public static void encodeToClassObject(Dataset<Row> data) {
		Dataset<Person> peopleDS = data.as(Encoders.bean(Person.class));
		peopleDS.show(100);
	}

	public static void conactColumnsValue(Dataset<Row> data, String column1, String column2) {
		Dataset<String> fullNameDS = data.map(
				(MapFunction<Row, String>) row -> row.getAs(column1).toString() + " " + row.getAs(column2).toString(),
				Encoders.STRING());
		fullNameDS.show();
	}

	public static void filterData(Dataset<Row> data, String column1, String value) {
		Dataset<Row> maleGenederDS = data.filter(col(column1).eqNullSafe(value));
		maleGenederDS.show();
	}

	public static void countGroupby(Dataset<Row> data, String cloumn1) {
		Dataset<Row> genderDS = data.groupBy(cloumn1).count();
		genderDS.show();
	}

	public static void selectColumnsData(Dataset<Row> data, String column1, String column2) {
		Dataset<Row> nameDS = data.select(col(column1), col(column2));
		nameDS.show();
	}

	public static void showData(Dataset<Row> data) {
		data.show();	
	}

	public static void showSchema(Dataset<Row> data) {
		data.printSchema();
	}
	
	public static void showSparkVersion(SparkSession spark) {
		System.out.println("Spark version*********" + spark.version());
	}
	
}
