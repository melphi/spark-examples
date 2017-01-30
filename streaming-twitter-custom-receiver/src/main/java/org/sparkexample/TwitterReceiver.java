package org.sparkexample;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Twitter stream receiver. Make sure resources/twitter4j.properties contains your Twitter
 * authentication values. {@see https://apps.twitter.com}.
 *
 * This receiver tracks the status messages containing the keyword twitter.
 */
public final class TwitterReceiver extends Receiver<Status> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterReceiver.class);

  /**
   * The keywords to be tracked.
   */
  private static final String KEYWORDS = "twitter";

  private final TwitterStream twitterStream;

  private StatusListener listener;

  public TwitterReceiver(StorageLevel storageLevel) {
    super(storageLevel);
    checkArgument(StorageLevel.MEMORY_ONLY().equals(storageLevel),
        String.format("Only [%s] supported.", StorageLevel.MEMORY_ONLY().toString()));
    twitterStream = new TwitterStreamFactory().getInstance();
  }

  @Override
  public void onStart() {
    if (listener == null) {
      listener = new StreamListener();
    }
    twitterStream.addListener(listener);
    twitterStream.filter(createFilter());
  }

  private FilterQuery createFilter() {
    FilterQuery filterQuery = new FilterQuery();
    try {
      filterQuery.track(KEYWORDS);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new IllegalArgumentException(e);
    }
    return filterQuery;
  }

  @Override
  public void onStop() {
    twitterStream.clearListeners();
    twitterStream.cleanUp();
    listener = null;
  }

  private class StreamListener implements StatusListener {
    @Override
    public void onStatus(Status status) {
      store(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
      // Intentionally empty.
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
      // Intentionally empty.
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
      // Intentionally empty.
    }

    @Override
    public void onStallWarning(StallWarning warning) {
      // Intentionally empty.
    }

    @Override
    public void onException(Exception ex) {
      LOGGER.warn(ex.getMessage(), ex);
    }
  }
}
