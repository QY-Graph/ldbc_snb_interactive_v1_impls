package org.ldbcouncil.snb.driver;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongHolder {
    // 定义一个 AtomicLong 类型的成员变量
    private final AtomicLong atomicLong = new AtomicLong();

    // 获取当前值
    public long get() {
        return atomicLong.get();
    }

    // 设置当前值
    public void set(long newValue) {
        atomicLong.set(newValue);
    }

    // 原子地将当前值与给定的值相加
    public void add(long delta) {
        atomicLong.addAndGet(delta);
    }

    // 原子地将当前值递增
    public void increment() {
        atomicLong.incrementAndGet();
    }

    // 原子地将当前值递减
    public void decrement() {
        atomicLong.decrementAndGet();
    }

    // 原子地更新值
    public boolean compareAndSet(long expectedValue, long newValue) {
        return atomicLong.compareAndSet(expectedValue, newValue);
    }

    // 打印当前值（辅助方法，用于演示）
    public void printValue() {
        System.out.println("Current value: " + atomicLong.get());
    }
}
