package com.sathish.spark;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sathish.spark.common.SparkConnection;

public class SparkSqlPractice {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkSession spSession = SparkConnection.getSession();
		JavaSparkContext context = SparkConnection.getContext();

		Dataset<Row> sampledataDF = spSession.read().option("header", true).csv("data/sampledata.csv");
		sampledataDF.printSchema();
		long leng = sampledataDF.filter(col("PetalWidth").gt("0.4")).count();
		
		System.out.println("no  of records" + "\t" + leng);

		JavaRDD<String> sampledataRdd = context.textFile("data/sampledata.csv");
		String header = sampledataRdd.first();
		JavaRDD<String> sampledataDataRdd = sampledataRdd.filter(str -> !str.equals(header));
		StructType sampledataschema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("SEPAL_LENGTH", DataTypes.DoubleType, false),
						DataTypes.createStructField("SEPAL_WIDTH", DataTypes.DoubleType, false),
						DataTypes.createStructField("PETAL_LENGTH", DataTypes.DoubleType, false),
						DataTypes.createStructField("PETAL_WIDTH", DataTypes.DoubleType, false),
						DataTypes.createStructField("SPECIES", DataTypes.StringType, false) });

		JavaRDD<Row> sampledataRowRdd = sampledataDataRdd.map(new Function<String, Row>() {

			@Override
			public Row call(String v1) throws Exception {

				String[] rowVal = v1.split(",");
				Row row = RowFactory.create(Double.valueOf(rowVal[0]), Double.valueOf(rowVal[1]),
						Double.valueOf(rowVal[2]), Double.valueOf(rowVal[3]), rowVal[4]);
				return row;
			}
		});

		Dataset<Row> sampledataDF1 = spSession.createDataFrame(sampledataRowRdd, sampledataschema);

		sampledataDF1.createOrReplaceTempView("sampledata");
		spSession.sql("select SPECIES,avg(PETAL_WIDTH) from sampledata group by SPECIES").show();
	}

}
