import org.junit.Test;
import org.sparkexample.TwitterStream;

public class TwitterStreamTest {
  @Test
  public void test() throws InterruptedException {
    new TwitterStream().run();
  }
}
