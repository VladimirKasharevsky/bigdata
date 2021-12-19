package com.example.wordcount;

import org.apache.spark.SparkConf;;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class SparkAndSms {
    private static void sms(String fileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sms");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        List<String> hamWords = inputFile
                .filter(conten -> conten.contains("ham\t"))
                .flatMap(content -> Arrays.asList(content.split(" ")))
                .collect();

        JavaRDD<String> spamWords = inputFile
                .filter(content -> content.contains("spam\t"))
                .flatMap(content -> Arrays.asList(content.split(" ")));

        Map<String, Integer> result = spamWords
                .filter(content -> !hamWords.contains(content))
                .mapToPair(t -> new Tuple2<String, Integer>(t, 1))
                .reduceByKey((x, y) -> (int) x + (int) y)
                .collectAsMap();

        Map<String, Integer> res = result.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new));


        System.out.println("Top 5 spam words are not contained in ham messages:");

        int limit = 0;
        for (String name: res.keySet()) {
            if(limit < 5) {
                String key = name.toString();
                String value = res.get(name).toString();
                System.out.println(key + " " + value);
                limit++;
            }
        }

        sparkContext.stop();
        sparkContext.close();
    }

    public static void main(String[] args) {
        sms("smsData.txt");
    }
}
