package com.taomee.bigdata.util;

import sun.misc.Signal;

public class Test extends TaomeeThread {
    public static void main(String[] args) {
        Test test = new Test();
        test.setSleepTime(300);
        test.run(args);
    }

    protected void process(String[] args) {
        System.out.println("process...");
    }

    protected void exit() {
        System.out.println("88");
    }
}
