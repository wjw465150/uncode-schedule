# uncode-schedule
基于Zookeeper+Quartz/SpringTask的分布式任务调度系统,非常小巧,零侵入,无需任何修改就可以使Quartz/SpringTask具备分布式特性,确保所有任务在集群中不重复,不遗漏的执行.

# 功能概述

1. 基于Zookeeper+Quartz/SpringTask的分布任务调度系统.
2. 确保每个任务在集群中不同节点上不重复的执行.
3. 单个任务节点故障时自动转移到其他任务节点继续执行.
4. 任务节点启动时必须保证zookeeper可用,任务节点运行期zookeeper集群不可用时任务节点保持悬挂状态,直到zookeeper集群恢复正常运期.
5. 任务会在各个任务节点均衡的执行.

说明:
* 所有任务节点的代码和task配置要完全一致!
* 单节点故障时需要业务保障数据完整性或幂等性.
* 具体使用方式和spring task/quartz相同,只需要配置ZKScheduleManager即可.
* 当添加,删除task时,当修改了task的逻辑时,要停止所有的任务节点,全部更新配置和代码后,再依次启动任务节点!  

------------------------------------------------------------------------

# 1. 基于Spring Task的XML配置

## 1.1 XML方式

1 Spring bean
```java
	public class SimpleTask {

		private static int i = 0;
		
		public void print() {
			System.out.println("===========start!=========");
			System.out.println("I:"+i);i++;
			System.out.println("=========== end !=========");
		}
	}
```
2 xml配置
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:p="http://www.springframework.org/schema/p"
	xmlns:task="http://www.springframework.org/schema/task"
	xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
        http://www.springframework.org/schema/task
        http://www.springframework.org/schema/task/spring-task-3.0.xsd">

	<!-- 分布式任务管理器 -->
	<bean id="zkScheduleManager" class="cn.uncode.schedule.ZKScheduleManager">
		<property name="zkConfig">
			<map>
				<entry key="zkConnectString" value="localhost:2181" />
				<entry key="rootPath" value="/schedule/dev" />
				<entry key="zkSessionTimeout" value="60000" />
				<entry key="userName" value="ScheduleAdmin" />
				<entry key="password" value="123456" />
				<entry key="autoRegisterTask" value="true" />
			</map>
		</property>
		<property name="reAssignTaskThreshold" value="10" />
	</bean>


	<!-- Spring task配置 -->
	<task:scheduled-tasks scheduler="zkScheduleManager">
		<task:scheduled ref="taskObj" method="print"
			fixed-rate="5000" />
	</task:scheduled-tasks>

	<!-- Spring bean配置 -->
	<bean id="taskObj" class="cn.uncode.schedule.test.SimpleTask" />
</beans>
```	
------------------------------------------------------------------------

## 1.2 Annotation方式

1 Spring bean
```java
	@Component
	public class SimpleTask {

		private static int i = 0;
		
		@Scheduled(fixedDelay = 1000) 
		public void print() {
			System.out.println("===========start!=========");
			System.out.println("I:"+i);i++;
			System.out.println("=========== end !=========");
		}
		
	}
```

2 xml配置
```xml
	<!-- 配置注解扫描 -->
    <context:annotation-config />
	<!-- 自动扫描的包名 -->
    <context:component-scan base-package="cn.uncode.schedule" />
    
	<!-- 分布式任务管理器 -->
	<bean id="zkScheduleManager" class="cn.uncode.schedule.ZKScheduleManager"
		init-method="init">
		<property name="zkConfig">
			   <map>
				  <entry key="zkConnectString" value="127.0.0.1:2181" />
				  <entry key="rootPath" value="/schedule/dev" />
				  <entry key="zkSessionTimeout" value="60000" />
				  <entry key="userName" value="ScheduleAdmin" />
				  <entry key="password" value="123456" />
				  <entry key="isCheckParentPath" value="true" />
			   </map>
		</property>
	</bean>
	
	<!-- Spring定时器注解开关-->
	<task:annotation-driven scheduler="zkScheduleManager" />
```
	
------------------------------------------------------------------------
	
# 2. 基于Quartz的XML配置
```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:p="http://www.springframework.org/schema/p"
	xmlns:task="http://www.springframework.org/schema/task"
	xsi:schemaLocation="http://www.springframework.org/schema/beans
        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
        http://www.springframework.org/schema/task
        http://www.springframework.org/schema/task/spring-task-3.0.xsd">


	<bean id="zkScheduleManager" class="cn.uncode.schedule.ZKScheduleManager">
		<property name="zkConfig">
			<map>
				<entry key="zkConnectString" value="127.0.0.1:2181" />
				<entry key="rootPath" value="/schedule/dev" />
				<entry key="zkSessionTimeout" value="60000" />
				<entry key="userName" value="ScheduleAdmin" />
				<entry key="password" value="123456" />
				<entry key="autoRegisterTask" value="true" />
			</map>
		</property>
		<property name="reAssignTaskThreshold" value="10" />
	</bean>

	<!-- Quartz SchedulerFactoryBean -->
	<bean id="startQuertz"
		class="org.springframework.scheduling.quartz.SchedulerFactoryBean">
		<property name="triggers">
			<list>
				<ref bean="Trigger_doTime" />
			</list>
		</property>
	</bean>

	<!-- Trigger_doTime -->
	<bean id="Trigger_doTime"
		class="org.springframework.scheduling.quartz.CronTriggerFactoryBean">
		<property name="jobDetail">
			<ref bean="JobDetail_jobtask" />
		</property>
		<!-- cron表达式 -->
		<property name="cronExpression">
			<value>0/3 * * * * ?</value>
		</property>
	</bean>
	<!-- JobDetail_jobtask -->
	<!-- 注意:spring的MethodInvokingJobDetailFactoryBean改成cn.uncode.schedule.quartz.MethodInvokingJobDetailFactoryBean -->
	<bean id="JobDetail_jobtask"
		class="cn.uncode.schedule.quartz.MethodInvokingJobDetailFactoryBean">
		<!-- 调用的类 -->
		<property name="targetObject" ref="taskObj" />
		<!-- 调用类中的方法 -->
		<property name="targetMethod" value="print" />
	</bean>

	<bean id="taskObj" class="cn.uncode.schedule.test.SimpleTask" />
</beans>
```	
------------------------------------------------------------------------

# 引用:
本代码基于`oschina:http://git.oschina.net/uncode/uncode-schedule`项目进行的改造,非常感谢!  
