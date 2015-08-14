package cn.uncode.schedule.zk;

import java.util.List;

/**
 * 调度配置中心客户端接口，可以有基于数据库的实现，可以有基于ConfigServer的实现
 * 
 * @author juny.ye
 * 
 */
public interface IScheduleDataManager {

  /**
   * 发送心跳信息
   * 
   * @param server
   * @throws Exception
   */
  public boolean refreshScheduleServer(ScheduleServer server) throws Exception;

  public void refreshScheduleTask(ScheduleTask task);

  /**
   * 注册服务器
   * 
   * @param server
   * @throws Exception
   */
  public void registerScheduleServer(ScheduleServer server) throws Exception;

  /**
   * 注销服务器
   * 
   * @param server
   * @throws Exception
   */
  public void UnRegisterScheduleServer(ScheduleServer server) throws Exception;

  public boolean isLeader(String uuid, List<String> serverList);

  /**
   * 清楚失效的ScheduleServer
   * 
   * @throws Exception
   */
  public void clearExpireScheduleServer() throws Exception;

  /**
   * 获取Zookeeper中的ScheduleServer列表
   * 
   * @return
   * @throws Exception
   */
  public List<String> loadScheduleServerNames() throws Exception;

  /**
   * 获取Zookeeper中的Task列表
   * 
   * @return
   * @throws Exception
   */
  public List<String> loadTaskNames() throws Exception;
  
  /**
   * 分配task给taskServerList中的随机一个!
   * @param currentUuid  ScheduleServer的UUID  
   * @param taskServerList ScheduleServer列表
   * @throws Exception
   */
  public void assignTask(String currentUuid, List<String> taskServerList) throws Exception;

  public boolean isOwner(String taskName, String uuid) throws Exception;

  public void addTask(String name) throws Exception;

  public void deleteTask(String name) throws Exception;
  
  public void deleteTaskOwner(String taskName, String uuid) throws Exception;
}