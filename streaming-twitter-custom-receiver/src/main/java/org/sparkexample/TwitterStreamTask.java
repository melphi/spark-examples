package org.sparkexample;

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;

/**
 * Twitter stream example.

 * Make sure resources/twitter4j.properties contains your Twitter authentication values.
 * {@see https://apps.twitter.com}
 */
public class TwitterStreamTask {
  /**
   * Kryo serializer offers much better performance than the default serializer.
   * {@see https://spark.apache.org/docs/latest/tuning.html#data-serialization}
   */
  private static final Class[] KRYO_CLASSES = ImmutableList.builder()
      .add(GeoLocation.class)
      .add(Status.class)
      .add(User.class)
      .build()
      .toArray(new Class[] {});

  /**
   * We use a logger to print the output. Sl4j is a common library which works with log4j, the
   * logging system used by Apache Spark.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterStreamTask.class);

  /**
   * This is the entry point function when the task is called with spark-submit.sh from command
   * line. In our example we will call the task from a WordCountTest instead.
   * See {@see http://spark.apache.org/docs/latest/submitting-applications.html}
   */
  public static void main(String args[]) throws InterruptedException {
    new TwitterStreamTask().run();
  }

  public void run() throws InterruptedException {
    /*
     * Creates a Spark local cluster with Kryo serialization enabled.
     */
    SparkConf conf = new SparkConf().setMaster("local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(KRYO_CLASSES)
        .setAppName("sparkTask");

    /*
     * Starts a streaming context with a windowing (micro batch) of 10 seconds.
     */
    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

    /*
     * Uses the custom Twitter receiver. For every micro batch prints the collected messages.
     *
     * coalesce(10) filters out empty micro batches by reducing the partitions.
     *
     * Make sure resources/twitter4j.properties contains your Twitter authentication values.
     * {@see https://apps.twitter.com}.
     */
    streamingContext.receiverStream(new TwitterReceiver(StorageLevel.MEMORY_ONLY()))
        .foreachRDD(
            rdd -> rdd.coalesce(10)
                .foreach(message -> LOGGER.info(message.getText())));

    /*
     * Starts the streaming task.
     */
    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
