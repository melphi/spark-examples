import org.junit.Test;
import org.sparkexample.WordCount;

import java.net.URISyntaxException;

public class WordCountTest {
  @Test
  public void test() throws URISyntaxException {
    String inputFile = getClass().getResource("loremipsum.txt").toURI().toString();
    new WordCount().run(inputFile);
  }
}
