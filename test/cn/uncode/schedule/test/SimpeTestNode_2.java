package cn.uncode.schedule.test;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author juny.ye
 */
public class SimpeTestNode_2 {

  public static void main(String[] args) throws InterruptedException {
    ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("applicationContext2.xml");

    Thread.sleep(300000 * 1000);

    context.stop();
    context.close();
    
    System.exit(0);
  }

}
