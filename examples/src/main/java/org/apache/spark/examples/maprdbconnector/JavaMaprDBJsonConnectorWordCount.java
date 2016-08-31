package org.apache.spark.examples.maprdbconnector;

import com.mapr.db.spark.api.java.MapRDBJavaSparkContext;
import com.mapr.db.spark.sql.api.java.MapRDBJavaSession;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;


public class JavaMaprDBJsonConnectorWordCount {

    public static void main(String[] args) {
        parseArgs(args);

        String pathToFileWithData = args[0];
        String tableName = args[1];
        String tableNameWithResult = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("OJAI MaprDB connector wordcount example")
                .getOrCreate();
        MapRDBJavaSession maprSession = new MapRDBJavaSession(spark);

        List<String> linesList = spark.sparkContext()
                .textFile(pathToFileWithData, 1)
                .toJavaRDD()
                .collect();

        List<Word> wordsList = linesList.stream().map(line -> {
            String[] wordsWithId = line.split(" ");
            String words = String.join(" ", Arrays.copyOfRange(wordsWithId, 1, wordsWithId.length));
            return new Word(wordsWithId[0], words);
        }).collect(Collectors.toList());

        Dataset<Word> wordsDf = spark.createDataset(wordsList, Encoders.bean(Word.class));

        maprSession.saveToMapRDB(wordsDf, tableName,"_id", true, true);

        Dataset<Word> dfWithDataFromMaprDB = maprSession.loadFromMapRDB(tableName, Word.class);

        FlatMapFunction<Word, String> lineWithWordsToList =
                new FlatMapFunction<Word,String>() {
                    @Override
                    public Iterator<String> call(Word word) {
                        List<String> eventList = Arrays.asList(word.getWords().split(" "));
                        return eventList.iterator();
                    }
                };

        Dataset<org.apache.spark.sql.Row> dfWithDataFromMaprDBcounted = dfWithDataFromMaprDB
                .flatMap(lineWithWordsToList, Encoders.STRING())
                .groupBy("value")
                .count();

        System.out.println("Dataset with counted words:");

        dfWithDataFromMaprDBcounted.show();

        maprSession.saveToMapRDB(
                dfWithDataFromMaprDBcounted.withColumn("_id", col("value")),
                tableNameWithResult,
                true);

        System.out.println("Dataset with counted words was saved into the MaprDB table.");

        spark.stop();
    }

    private static void parseArgs(String[] args) {
        if (args.length != 3) {
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("OJAI MaprDB connector wordcount example\n" +
                "Usage:\n" +
                "1) path to the file with data (words.txt can be used for the test);\n" +
                "   by default Spark will search file in maprfs. If you want to use local file\n" +
                "   you need to add 'file:///' before a path to a file;" +
                "2) name of the MaprDB table where data from file will be saved;\n" +
                "3) name of the MaprDB table where result will be saved;");
    }

    public static class Word implements Serializable {

        private String _id;
        private String words;

        public Word() { }
        public Word(String _id, String words) {
            this._id = _id;
            this.words = words;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public String getWords() {
            return words;
        }

        public void setWords(String words) {
            this.words = words;
        }
    }

}
