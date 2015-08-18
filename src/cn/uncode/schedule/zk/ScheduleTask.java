package cn.uncode.schedule.zk;

import java.sql.Timestamp;

/**
 * Task信息定义
 * 
 * @author wjw465150@gmail.com
 * 
 */
public class ScheduleTask {
  private String name;
  private String uuid;
  private String desc;
  private Timestamp lastfireTime;

  public ScheduleTask(String name, String uuid, String desc, Timestamp lastfireTime) {
    super();
    this.name = name;
    this.uuid = uuid;
    this.desc = desc;
    this.lastfireTime = lastfireTime;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  public Timestamp getLastfireTime() {
    return lastfireTime;
  }

  public void setLastfireTime(Timestamp lastfireTime) {
    this.lastfireTime = lastfireTime;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ScheduleTask [name=");
    builder.append(name);
    builder.append(", uuid=");
    builder.append(uuid);
    builder.append(", desc=");
    builder.append(desc);
    builder.append(", lastfireTime=");
    builder.append(lastfireTime);
    builder.append("]");
    return builder.toString();
  }
}
