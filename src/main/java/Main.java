/**
 * Simple HelloWorld-esque program.
 */
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class Main {
    public static String SPARK_HOME;
    public static String logFile;
    public static String resultsFile;
    public static int numThreads;

    public static long countStringOccurence(JavaRDD<String> rdd, String s) {
        return rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains(s);
            }
        }).count();
    }

    public static void reportResults(String fileName, String results) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("results.txt"));
            writer.write(results);
            writer.close();
        } catch (IOException ioe) {
            System.err.format("IOException: %s%n", ioe);
        }
    }

    public static void main(String[] args) {
        // obtain the SPARK_HOME environment variable and set appropriate file paths
//        Map<String, String> env = System.getenv();
//        SPARK_HOME = env.get("SPARK_HOME");
        SPARK_HOME = "/usr/lib/spark";
        logFile = SPARK_HOME + "/README.md";
        resultsFile = "results.txt";
        numThreads = 2;

        // set up the spark master as the local machine with the desired number of threads
        SparkConf config = new SparkConf()
                .setAppName("#tomatoGetsSparky")
                .setMaster("local[" + numThreads + "]");
        JavaSparkContext sparkContext = new JavaSparkContext(config);

        // count the occurrences of the desired strings in the specified log file
        JavaRDD<String> logData = sparkContext.textFile(logFile).cache();
        long numAs = Main.countStringOccurence(logData, "a");
        long numBs = Main.countStringOccurence(logData, "b");
        Main.reportResults(resultsFile, "Lines with a: " + numAs + "\nLines with b: " + numBs + "\n");
    }
}
