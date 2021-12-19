package com.example.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCounter {
    private static void wordCount(String fileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        List<String> stopWord = Arrays.asList("ready", "are");

        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));

        JavaPairRDD<String, Integer> countData = wordsFromFile
                .filter(content -> !stopWord.contains(content))
                .map(string -> string.toLowerCase())
                .mapToPair(t -> new Tuple2<String, Integer>(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y);


        countData.saveAsTextFile("CountData");

        sparkContext.stop();
        sparkContext.close();
    }


    public static void main(String[] args) {
        wordCount("input.txt");
    }
}
