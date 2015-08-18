package cn.uncode.schedule;

import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.util.Assert;

import cn.uncode.schedule.util.ScheduleUtil;
import cn.uncode.schedule.zk.IScheduleDataManager;
import cn.uncode.schedule.zk.ScheduleDataManager4ZK;
import cn.uncode.schedule.zk.ScheduleServer;
import cn.uncode.schedule.zk.ScheduleTask;
import cn.uncode.schedule.zk.ZKManager;

/**
 * 调度器核心管理
 * 
 * @author juny.ye
 * 
 */
public class ZKScheduleManager extends ThreadPoolTaskScheduler implements ApplicationContextAware {

  private static final long serialVersionUID = 1L;

  protected static final transient Logger LOG = LoggerFactory.getLogger(ZKScheduleManager.class);

  private Map<String, String> zkConfig;

  protected ZKManager zkManager;

  private IScheduleDataManager scheduleDataManager;

  private static ZKScheduleManager instance = null;

  /**
   * 当前调度服务的信息
   */
  protected ScheduleServer currenScheduleServer;

  /**
   * 是否启动调度管理，如果只是做系统管理，应该设置为false
   */
  public boolean start = true;

  /**
   * 心跳间隔
   */
  private int timerInterval = 3000;

  /**
   * 重新分配task的阀值
   */
  private int reAssignTaskThreshold = 10;

  public int getReAssignTaskThreshold() {
    return reAssignTaskThreshold;
  }

  public void setReAssignTaskThreshold(int reAssignTaskThreshold) {
    Assert.isTrue((reAssignTaskThreshold > 0));
    this.reAssignTaskThreshold = reAssignTaskThreshold;
  }

  /**
   * 是否注册成功
   */
  private boolean registed = false;

  private ApplicationContext applicationcontext;

  //task运行次数Map
  private Map<String, Integer> _taskRunCountMap = new ConcurrentHashMap<String, Integer>();

  public Map<String, Integer> getTaskRunCountMap() {
    return _taskRunCountMap;
  }

  private Timer hearBeatTimer;
  protected Lock initLock = new ReentrantLock();
  protected boolean isStopSchedule = false;
  protected Lock registerLock = new ReentrantLock();

  volatile String errorMessage = "No config Zookeeper connect infomation";
  private InitialThread initialThread;

  public ZKScheduleManager() {
    this.currenScheduleServer = ScheduleServer.createScheduleServer(null);
  }

  public static ZKScheduleManager getInstance() {
    return instance;
  }

  @Override
  public void afterPropertiesSet() {
    super.afterPropertiesSet();

    try { //@wjw_note: 初始化_taskRunCountMap
      SchedulerFactoryBean schedulerFactoryBean = applicationcontext.getBean(org.springframework.scheduling.quartz.SchedulerFactoryBean.class);
      Scheduler scheduler = schedulerFactoryBean.getScheduler();
      String[] triggerNames = scheduler.getTriggerNames(Scheduler.DEFAULT_GROUP);
      for (String triggerName : triggerNames) {
        org.quartz.Trigger trigger = scheduler.getTrigger(triggerName, Scheduler.DEFAULT_GROUP);
        String taskName = trigger.getName() + "." + trigger.getJobName();
        _taskRunCountMap.put(taskName, 0);
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }

    Properties properties = new Properties();
    for (Map.Entry<String, String> e : this.zkConfig.entrySet()) {
      properties.put(e.getKey(), e.getValue());
    }

    try {
      this.init(properties);
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
  }

  public void reInit(Properties properties) throws Exception {
    if (this.start == true || this.hearBeatTimer != null) {
      throw new Exception("调度器有任务处理，不能重新初始化");
    }
    this.init(properties);
  }

  public void init(Properties properties) throws Exception {
    if (this.initialThread != null) {
      this.initialThread.stopThread();
    }
    this.initLock.lock();
    try {
      this.scheduleDataManager = null;
      instance = this;
      if (this.zkManager != null) {
        this.zkManager.close();
      }
      this.zkManager = new ZKManager(properties);
      while (this.zkManager.isZookeeperConnected() == false) {
        LOG.info("等待连接上Zookeeper......");
        TimeUnit.SECONDS.sleep(1);
      }

      this.errorMessage = "Zookeeper connecting ......" + this.zkManager.getConnectStr();
      initialThread = new InitialThread(this);
      initialThread.setName("ScheduleManager-initialThread");
      initialThread.start();
    } finally {
      this.initLock.unlock();
    }
  }

  @Override
  public void destroy() {
    try {
      if (this.initialThread != null) {
        try {
          this.initialThread.stopThread();
        } finally {
          this.initialThread = null;
        }
      }

      if (this.hearBeatTimer != null) {
        try {
          this.hearBeatTimer.cancel();
        } finally {
          this.hearBeatTimer = null;
        }
      }

      if (this.scheduleDataManager != null) {
        try {
          this.scheduleDataManager.UnRegisterScheduleServer(this.currenScheduleServer);
          this.scheduleDataManager.clearExpireScheduleServer();
        } finally {
          this.scheduleDataManager = null;
        }
      }

      if (this.zkManager != null) {
        try {
          this.zkManager.close();
        } finally {
          this.zkManager = null;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      super.destroy();
    }
  }

  public void rewriteScheduleInfo() throws Exception {
    registerLock.lock();
    try {
      if (this.isStopSchedule == true) {
        if (LOG.isDebugEnabled()) {
          LOG.warn("外部命令终止调度,不在注册调度服务，避免遗留垃圾数据：" + currenScheduleServer.getUuid());
        }
        return;
      }

      // 先发送心跳信息
      if (errorMessage != null) {
        this.currenScheduleServer.setDealInfoDesc(errorMessage);
      }
      if (this.scheduleDataManager.refreshScheduleServer(this.currenScheduleServer) == false) {
        // 更新信息失败，清除内存数据后重新注册
        this.clearMemoInfo();
        this.scheduleDataManager.registerScheduleServer(this.currenScheduleServer);
      }
      this.registed = true;
    } finally {
      registerLock.unlock();
    }
  }

  /**
   * 清除内存中所有的已经取得的数据和任务队列.在状态更新失败，或者发现注册中心的调度信息被删除时调用此方法!
   */
  public void clearMemoInfo() {
    try {

    } finally {
    }

  }

  /**
   * 根据当前调度服务器的信息，重新计算分配所有的调度任务. 任务的分配是需要加锁，避免数据分配错误。为了避免数据锁带来的负面作用，通过版本号来达到锁的目的
   * 
   * 1、获取任务状态的版本号 2、获取所有的服务器注册信息和任务队列信息 3、清除已经超过心跳周期的服务器注册信息 3、重新计算任务分配
   * 4、更新任务状态的版本号【乐观锁】 5、更新任务队列的分配信息
   * 
   * @throws Exception
   */
  public void assignScheduleTask() throws Exception {
    scheduleDataManager.clearExpireScheduleServer(); //@wjw_note: 先清理无效的ScheduleServer!

    List<String> serverList = scheduleDataManager.loadScheduleServerNames();
    if (scheduleDataManager.isLeader(this.currenScheduleServer.getUuid(), serverList) == false) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(this.currenScheduleServer.getUuid() + ":不是负责任务分配的Leader,直接返回");
      }
      return;
    }

    //@wjw_note: 添加主动清理Quartz类型的遗留下来的垃圾task
    if (_taskRunCountMap.size() > 0) {
      List<String> zkTaskNames = scheduleDataManager.loadTaskNames();
      for (String zkTaskName : zkTaskNames) {
        if (_taskRunCountMap.containsKey(zkTaskName) == false) {
          LOG.warn("删除垃圾Task:[" + zkTaskName + "]");
          scheduleDataManager.deleteTask(zkTaskName);
        }
      }
    }

    // 设置初始化成功标准，避免在leader转换的时候，新增的线程组初始化失败
    scheduleDataManager.assignTask(this.currenScheduleServer.getUuid(), serverList);
  }

  /**
   * 定时向Zookeeper更新当前服务器的心跳信息。<br/>
   * 如果发现本次更新的时间如果已经超过了服务器死亡的心跳周期，
   * 则不能再向Zookeeper更新信息,而应该当作新的ScheduleServer，进行重新注册。
   * 
   * @throws Exception
   */
  public void refreshScheduleServer() throws Exception {
    try {
      rewriteScheduleInfo();
      // 如果任务信息没有初始化成功，不做任务相关的处理
      if (this.registed == false) {
        return;
      }

      // 重新分配任务
      this.assignScheduleTask();
    } catch (Throwable e) {
      // 清除内存中所有的已经取得的数据和任务队列,避免心跳线程失败时候导致的数据重复
      this.clearMemoInfo();
      if (e instanceof Exception) {
        throw (Exception) e;
      } else {
        throw new Exception(e.getMessage(), e);
      }
    }
  }

  /**
   * 在Zk状态正常后回调数据初始化
   * 
   * @throws Exception
   */
  public void initialData() throws Exception {
    this.zkManager.initial();
    this.scheduleDataManager = new ScheduleDataManager4ZK(this.zkManager);
    if (this.start == true) {
      // 注册调度管理器
      this.scheduleDataManager.registerScheduleServer(this.currenScheduleServer);
      if (hearBeatTimer == null) {
        hearBeatTimer = new Timer("ScheduleManager-" + this.currenScheduleServer.getUuid() + "-HearBeat");
      }
      hearBeatTimer.schedule(new HeartBeatTimerTask(this), 2000, this.timerInterval);
    }
  }

  private Runnable taskWrapper(final Runnable task) {
    return new Runnable() {
      public void run() {
        ScheduledMethodRunnable scheduledMethodRunnable = (ScheduledMethodRunnable) task;
        Method targetMethod = scheduledMethodRunnable.getMethod();
        String[] beanNames = applicationcontext.getBeanNamesForType(targetMethod.getDeclaringClass());
        if (null != beanNames && StringUtils.isNotEmpty(beanNames[0])) {
          String taskName = "SpringScheduler." + ScheduleUtil.getTaskNameFormBean(beanNames[0], targetMethod.getName());
          if (_taskRunCountMap.containsKey(taskName) == false) {
            _taskRunCountMap.put(taskName, 0);
          }
          boolean isOwner = false;
          try {
            if (registed == false) {
              Thread.sleep(1000);
            }
            if (zkManager.isZookeeperConnected()) {
              String taskDesc = "Spring:Task";
              ScheduleTask scheduleTask = new ScheduleTask(taskName, currenScheduleServer.getUuid(), taskDesc, new Timestamp(System.currentTimeMillis()));

              isOwner = scheduleDataManager.isOwner(scheduleTask);
            }
          } catch (Exception e) {
            LOG.error("Check task owner error.", e);
          }

          if (isOwner) { //@wjw_note: 如果是task的owner,就执行!
            int fireCount = _taskRunCountMap.get(taskName);
            fireCount++;
            _taskRunCountMap.put(taskName, fireCount);

            task.run();
            LOG.info("Cron job has been executed.");

            //@wjw_note: 添加让出逻辑!
            if ((fireCount % ZKScheduleManager.getInstance().getReAssignTaskThreshold()) == 0) {
              LOG.debug("Task执行次数已经达到让出阀值:[" + fireCount + "],让出执行权给其他节点!");
              try {
                ZKScheduleManager.getInstance().getScheduleDataManager().deleteTaskOwner(taskName, ZKScheduleManager.getInstance().getScheduleServerUUid());
              } catch (Exception ex) {
                LOG.error(ex.getMessage(), ex);
              }
            }
          }
        }
      }
    };
  }

  class HeartBeatTimerTask extends java.util.TimerTask {
    private transient final Logger log = LoggerFactory.getLogger(HeartBeatTimerTask.class);
    ZKScheduleManager manager;

    public HeartBeatTimerTask(ZKScheduleManager aManager) {
      manager = aManager;
    }

    public void run() {
      try {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        manager.refreshScheduleServer();
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
      }
    }
  }

  class InitialThread extends Thread {
    private transient Logger log = LoggerFactory.getLogger(InitialThread.class);
    ZKScheduleManager sm;

    public InitialThread(ZKScheduleManager sm) {
      this.sm = sm;
    }

    boolean isStop = false;

    public void stopThread() {
      this.isStop = true;
    }

    @Override
    public void run() {
      sm.initLock.lock();
      try {
        int count = 0;
        while (sm.zkManager.isZookeeperConnected() == false) {
          count = count + 1;
          if (count % 50 == 0) {
            sm.errorMessage = "Zookeeper connecting ......" + sm.zkManager.getConnectStr() + " spendTime:" + count * 20 + "(ms)";
            log.error(sm.errorMessage);
          }
          Thread.sleep(20);
          if (this.isStop == true) {
            return;
          }
        }
        sm.initialData();
      } catch (Throwable e) {
        log.error(e.getMessage(), e);
      } finally {
        sm.initLock.unlock();
      }

    }

  }

  public IScheduleDataManager getScheduleDataManager() {
    return scheduleDataManager;
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationcontext)
      throws BeansException {
    this.applicationcontext = applicationcontext;
  }

  public void setZkManager(ZKManager zkManager) {
    this.zkManager = zkManager;
  }

  public ZKManager getZkManager() {
    return zkManager;
  }

  public void setZkConfig(Map<String, String> zkConfig) {
    this.zkConfig = zkConfig;
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
    return super.scheduleAtFixedRate(taskWrapper(task), period);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
    return super.schedule(taskWrapper(task), trigger);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
    return super.schedule(taskWrapper(task), startTime);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
    return super.scheduleAtFixedRate(taskWrapper(task), startTime, period);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
    return super.scheduleWithFixedDelay(taskWrapper(task), startTime, delay);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
    return super.scheduleWithFixedDelay(taskWrapper(task), delay);
  }

  public String getScheduleServerUUid() {
    if (null != currenScheduleServer) {
      return currenScheduleServer.getUuid();
    }
    return null;
  }

  public boolean isRegisted() {
    return registed;
  }
}