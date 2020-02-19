package com.yuepengfei.thread;


import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadDemo {

    public static void test(){
        String name = Thread.currentThread().getName();
        System.out.println("线程： "+name);
    }

    public static void main(String[] args) {

        Thread main = Thread.currentThread();

        new Thread(() ->{
            test();
            LockSupport.unpark(main);
        }).start();

        LockSupport.park();

        test();
    }

}

class ThreadDemo1 {

    volatile int count;

    ReentrantLock lock = new ReentrantLock(true);


    public void test0(){
        String name = Thread.currentThread().getName();
        count++;
        System.out.println("线程: "+ name + "------------" + count);
    }


    public void test1(){
        lock.lock();
        String name = Thread.currentThread().getName();
        count++;
        System.out.println("线程: "+ name + "------------" + count);
        lock.unlock();

    }

    public synchronized void test2(){
        String name = Thread.currentThread().getName();
        count++;
        System.out.println("线程: "+ name + "------------" + count);
    }



    public static void main(String[] args) {
        ThreadDemo1 demo1 = new ThreadDemo1();
        int i = 0;
        while (i < 10){
            i++;
            new Thread(() -> {
                while (demo1.count < 991) {
                    demo1.test1();
                }
            }).start();
        }
    }

}

