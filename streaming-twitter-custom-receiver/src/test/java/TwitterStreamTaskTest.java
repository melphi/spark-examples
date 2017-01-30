import org.junit.Test;
import org.sparkexample.TwitterStreamTask;

public class TwitterStreamTaskTest {
  @Test
  public void test() throws InterruptedException {
    new TwitterStreamTask().run();
  }
}
