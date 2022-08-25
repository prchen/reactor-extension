package com.github.prchen.reactor.fusion;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author prchen
 */
public class FusionFactory {
    private static final AtomicInteger FACTORY_COUNTER = new AtomicInteger();
    private final ExecutorService exec;
    private final List<FusionReactor<?, ?>> reactors = new ArrayList<>();

    public FusionFactory() {
        String threadPrefix = String.format("fr-%d-", FACTORY_COUNTER.getAndIncrement());
        AtomicInteger threadCounter = new AtomicInteger();
        this.exec = new ThreadPoolExecutor(
                0, Integer.MAX_VALUE, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                runnable -> new Thread(runnable, threadPrefix + threadCounter.getAndIncrement()),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public synchronized <T, U> FusionReactor<T, U> getInstance(FusionReaction<T, U> handler) {
        FusionReactor<T, U> reactor = new FusionReactor<>(exec, handler);
        reactors.add(reactor);
        return reactor;
    }

    public synchronized boolean shutdown(Duration timeout) throws InterruptedException {
        exec.shutdown();
        reactors.forEach(FusionReactor::triggerLoop);
        return exec.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public synchronized void shutdownNow() {
        exec.shutdownNow();
    }
}
