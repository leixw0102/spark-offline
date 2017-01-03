package com.ehl.offline.metric;

import com.codahale.metrics.*;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by 雷晓武 on 2016/12/30.
 */
public class GetStarted {

    private final Queue queue;

    public GetStarted( String name) {
        this.queue = new ArrayBlockingQueue(100);
        Gauge a=metrics.register(MetricRegistry.name(GetStarted.class, name, "size"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return queue.size();
                    }
                });
        System.out.print(a.getValue());
//        queue.add("sdf");
    }

    static final MetricRegistry metrics = new MetricRegistry();
    public static void main(String args[]) throws InterruptedException {
        startJMXReport();

        new GetStarted("leixw");
        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new GarbageCollectorMetricSet());
        Meter requests = metrics.meter("requests");
        requests.mark();
        Thread.sleep(10*30*30*40L);
    }
    static void startJMXReport(){
        final JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
        reporter.start();
    }
    static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    static void wait5Seconds() {
        try {
            Thread.sleep(5*1000);
        }
        catch(InterruptedException e) {}
    }
}
