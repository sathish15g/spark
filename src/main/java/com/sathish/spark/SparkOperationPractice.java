package com.sathish.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;

import com.sathish.spark.common.CommonUtils;
import com.sathish.spark.common.SparkConnection;

import scala.Tuple2;

public class SparkOperationPractice {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		System.out.println("*********************************LOADING DATA*****************************");
		JavaSparkContext javaSparkContext = SparkConnection.getContext();
		JavaRDD<String> sampledataRDD = javaSparkContext.textFile("data/sampledata.csv");
		JavaRDD<String> newsampledata = sampledataRDD.map(str -> str.toUpperCase());
		CommonUtils.printStringRDD(newsampledata, 3);

		System.out.println("*********************************TRANSFORMATION PHASE*****************************");
		JavaRDD<String> versicolorRDD = sampledataRDD.filter(str -> str.contains("versicolor"));
		CommonUtils.printStringRDD(versicolorRDD, (int) versicolorRDD.count());
		JavaRDD<String> numericVersicolorRDD = versicolorRDD.map(new Function<String, String>() {

			@Override
			public String call(String v1) throws Exception {

				String[] v1Split = v1.split(",");
				v1Split[4] = v1Split[4].toUpperCase();
				for (int i = 0; i < v1Split.length - 1; i++) {
					v1Split[i] = String.valueOf(Float.parseFloat(v1Split[i]));
				}
				String result = v1Split[0];
				for (int i = 1; i < v1Split.length; i++) {
					result = result + "," + v1Split[i];
				}
				return result;
			}
		});
		
		CommonUtils.printStringRDD(numericVersicolorRDD, 10);

		System.out.println("*********************************ACTION PHASE********************************");

		JavaRDD<String> sampledataDataRDD = sampledataRDD.filter(str -> !str.contains("SepalLength"));

		String sumOfSepal = sampledataDataRDD.reduce(new Function2<String, String, String>() {

			@Override
			public String call(String v1, String v2) throws Exception {
				String val1[], val2[];
				String sepalVal1, sepalVal2;
				if (v1.contains(",")) {
					val1 = v1.split(",");
					sepalVal1 = val1[0];
				} else {
					sepalVal1 = v1;
				}

				if (v2.contains(",")) {
					val2 = v2.split(",");
					sepalVal2 = val2[0];
				} else {
					sepalVal2 = v2;
				}

				double sum = Double.valueOf(sepalVal1) + Double.valueOf(sepalVal2);
				return String.valueOf(sum);
			}
		});

		System.out.println("**********AVERAGE OF SEPAL IS:" + Double.parseDouble(sumOfSepal) / (sampledataRDD.count() - 1));

		System.out.println("*********************************KV PAIR********************************");

		JavaPairRDD<String, Double> keyValPairs = sampledataDataRDD.mapToPair(new GetKV());

		for (Tuple2<String, Double> keyValPair : keyValPairs.take(10)) {
			System.out.println(keyValPair._1 + " - " + keyValPair._2);
		}
		
		JavaPairRDD<String, Double> reducedKeyVal = keyValPairs.reduceByKey(new Function2<Double, Double, Double>() {
			
			@Override
			public Double call(Double v1, Double v2) throws Exception {

				return (v1<v2?v1:v2);
			}
		});
		

		for (Tuple2<String, Double> keyValPair : reducedKeyVal.take(10)) {
			System.out.println(keyValPair._1 + " - " + keyValPair._2);
		}

		System.out.println("***********************************BRODCAST & ACCUMULATOR**********************************");
		Broadcast<Double> avrg = javaSparkContext.broadcast(Double.parseDouble(sumOfSepal) / (sampledataRDD.count() - 1));
		DoubleAccumulator accumulator = javaSparkContext.sc().doubleAccumulator();
		
		JavaRDD<String> autoCount = sampledataDataRDD.map(new Function<String, String>() {

			@Override
			public String call(String v1) throws Exception {

				String[] arry = v1.split(",");
				Double sepaLen = Double.valueOf(arry[0]);
				if(avrg.getValue().compareTo(sepaLen)<0){
					accumulator.add(1);
				}
				return arry[0];
			}
		});
		
		autoCount.count();
		System.out.println("Accumalated no of sep len grt than avg "+accumulator.value());
	}

}

class GetKV implements PairFunction<String, String, Double> {

	@Override
	public Tuple2<String, Double> call(String t) throws Exception {

		String[] s = t.split(",");
		String key = s[4];
		Double val = Double.parseDouble(s[0]);
		return new Tuple2<String, Double>(key, val);
	}

}
