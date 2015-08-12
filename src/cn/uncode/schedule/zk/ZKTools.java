package cn.uncode.schedule.zk;

import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * zk工具类
 * 
 * @author juny.ye
 * 
 */
public class ZKTools {
  public static void createPath(ZooKeeper zk, String path, CreateMode createMode, List<ACL> acl) throws Exception {
    String[] list = path.split("/");
    String zkPath = "";
    for (String str : list) {
      if (str.equals("") == false) {
        zkPath = zkPath + "/" + str;
        if (zk.exists(zkPath, false) == null) {
          zk.create(zkPath, null, acl, createMode);
        }
      }
    }
  }

  public static void printTree(ZooKeeper zk, String path, Writer writer, String lineSplitChar) throws Exception {
    String[] list = getSortedTree(zk, path);
    Stat stat = new Stat();
    for (String name : list) {
      byte[] value = zk.getData(name, false, stat);
      if (value == null) {
        writer.write(name + lineSplitChar);
      } else {
        writer.write(name + "[v." + stat.getVersion() + "]" + "[" + new String(value) + "]" + lineSplitChar);
      }
    }
  }

  //返回排序好的Path数组
  public static String[] getSortedTree(ZooKeeper zk, String path) throws Exception {
    if (zk.exists(path, false) == null) {
      return new String[0];
    }

    List<String> dealList = new ArrayList<String>();
    dealList.add(path);

    int index = 0;
    while (index < dealList.size()) {
      String tempPath = dealList.get(index);
      List<String> children = zk.getChildren(tempPath, false);
      if (tempPath.equalsIgnoreCase("/") == false) {
        tempPath = tempPath + "/";
      }
      Collections.sort(children);
      for (int i = children.size() - 1; i >= 0; i--) {
        dealList.add(index + 1, tempPath + children.get(i));
      }
      index++;
    }
    return (String[]) dealList.toArray(new String[0]);
  }
  
  public static void deleteTree(ZooKeeper zk, String path) throws Exception {
    //    String[] list = getSortedTree(zk, path);
    //    for (int i = list.length - 1; i >= 0; i--) {
    //      zk.delete(list[i], -1);
    //    }

    List<String> tree = listSubTreeBFS(zk, path);
    for (int i = tree.size() - 1; i >= 0; i--) { //@wjw_note: 必须倒序!
      zk.delete(tree.get(i), -1);
    }
  }

  /**
   * BFS Traversal of the system under pathRoot, with the entries in the list,
   * in the same order as that of the traversal.
   * <p>
   * <b>Important:</b> This is <i>not an atomic snapshot</i> of the tree ever,
   * but the state as it exists across multiple RPCs from zkClient to the
   * ensemble. For practical purposes, it is suggested to bring the clients to
   * the ensemble down (i.e. prevent writes to pathRoot) to 'simulate' a
   * snapshot behavior.
   * 
   * @param zk
   * @param pathRoot
   *          The znode path, for which the entire subtree needs to be listed.
   * @throws Exception
   */
  public static List<String> listSubTreeBFS(ZooKeeper zk, final String pathRoot) throws Exception {
    Deque<String> queue = new LinkedList<String>();
    List<String> tree = new ArrayList<String>();
    queue.add(pathRoot);
    tree.add(pathRoot);

    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }

      List<String> children = zk.getChildren(node, false);
      for (final String child : children) {
        final String childPath = node + "/" + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }

}
