package cn.uncode.schedule;

import java.io.StringWriter;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import cn.uncode.schedule.zk.ZKTools;

/**
 * @author juny.ye
 */
public class ZookeeperTest {
  private PrintWatcher watcher = new PrintWatcher();

  @Test
  public void testCloseStatus() throws Exception {
    ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, watcher);
    int i = 1;
    while (true) {
      try {
        StringWriter writer = new StringWriter();
        ZKTools.printTree(zk, "/schedule/dev", writer, "");
        System.out
            .println(i++ + "----" + writer.getBuffer().toString());
        Thread.sleep(2000);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
  }

  @Test
  public void testPrint() throws Exception {
    ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, watcher);
    StringWriter writer = new StringWriter();
    ZKTools.printTree(zk, "/", writer, "\n");
    System.out.println(writer.getBuffer().toString());

    zk.close();
  }

  @Test
  public void deletePath() throws Exception {
    ZooKeeper zk = new ZooKeeper("localhost:2181", 3000, watcher);
    zk.addAuthInfo("digest", "ScheduleAdmin:123456".getBytes());

    ZKTools.deleteTree(zk, "/schedule/dev");

    Thread.sleep(10 * 1000);
    zk.close();
  }

  private static class PrintWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      System.out.println(event);
    }
  }
}