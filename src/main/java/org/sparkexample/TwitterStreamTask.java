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

public class TwitterStreamTask {

  private static final Class[] KRYO_CLASSES = ImmutableList.builder()
      .add(GeoLocation.class)
      .add(Status.class)
      .add(User.class)
      .build()
      .toArray(new Class[] {});

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterStreamTask.class);

  public static void main(String args[]) throws InterruptedException {
    new TwitterStreamTask().run();
  }

  public void run() throws InterruptedException {

    SparkConf conf = new SparkConf().setMaster("local[*]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(KRYO_CLASSES)
        .setAppName("sparkTask");

    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

    streamingContext.receiverStream(new TwitterReceiver(StorageLevel.MEMORY_ONLY()))
        .foreachRDD(
            rdd -> rdd.coalesce(10)
                .foreach(message -> LOGGER.info(message.getText())));

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
