package com.zhyf.window;

/**
 * eventTime 滚动窗口时间的范围边界我
 * 1. flink的时间精度是1ms
 * 2. flink的窗口起始时间结束时间是对其的 （窗口的起始时间和结束时间是窗口长度的整数倍）
 * 3. flink的窗口范围是前闭后开的范围[5000,10000) 或 [5000,9999]
 * 4. waterMark是flink中的一种特殊数据 主要解决了分布式窗口统一触发和数据乱序延迟 容忍数据迟到一定的时间
 *   每个分区的waterMark最大的eventTime - 延迟时间
 *   如果生成waterMark的dataStream只有一个分区 那么只要该分区的waterMark >= 窗口结束时间的闭区间 窗口就触发
 *
 */
public class EventTimeTumblingWindowBound {
    public static void main(String[] args) {

    }
}
