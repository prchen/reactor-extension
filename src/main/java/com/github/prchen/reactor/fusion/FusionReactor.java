package com.github.prchen.reactor.fusion;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @param <T> t
 * @param <U> u
 * @author prchen
 */
public class FusionReactor<T, U> {
    private final Semaphore semaphore = new Semaphore(0);
    private final Map<Thread, List<Tuple2<T, MonoSink<U>>>> queues = new ConcurrentHashMap<>();
    private final ExecutorService exec;
    private final FusionReaction<T, U> reaction;

    FusionReactor(ExecutorService exec, FusionReaction<T, U> reaction) {
        this.exec = exec;
        this.reaction = reaction;
        exec.submit(this::coreLoop);
    }

    public Mono<U> react(T atom) {
        return Mono.create(sink -> {
            List<Tuple2<T, MonoSink<U>>> queue = new ArrayList<>();
            queue.add(Tuples.of(atom, sink));
            queues.merge(Thread.currentThread(), queue, (oldQueue, newQueue) -> {
                oldQueue.addAll(newQueue);
                return oldQueue;
            });
            if (semaphore.availablePermits() == 0) {
                semaphore.release();
            }
        });
    }

    void triggerLoop() {
        semaphore.release();
    }

    private void coreLoop() {
        while (!exec.isShutdown()) {
            semaphore.acquireUninterruptibly(Integer.max(1, semaphore.availablePermits()));
            AtomicInteger identifier = new AtomicInteger();
            Flux.fromIterable(new ArrayList<>(queues.keySet()))
                    .flatMapIterable(queues::remove)
                    .collectMap(reqPair -> identifier.getAndIncrement(), Function.identity())
                    .filter(fuel -> !fuel.isEmpty())
                    .flatMapMany(this::react)
                    .blockLast();
        }
    }

    private Flux<MonoSink<U>> react(Map<Integer, Tuple2<T, MonoSink<U>>> fuel) {
        // Call upstream method
        return Flux.fromIterable(fuel.entrySet())
                .collectMap(Map.Entry::getKey, entry -> entry.getValue().getT1())
                .map(reaction::react)
                // Fallback to Flux with Mono of error when upstream method throws an exception directly
                .onErrorResume(e -> Mono.just(Flux.fromIterable(fuel.keySet())
                        .map(id -> Tuples.of(id, Mono.<U>error(e)))))
                // Map to (id, responseMono, responseSink) triad and send results
                .flatMapMany(Function.identity())
                .flatMap(resPair -> Mono.justOrEmpty(fuel.get(resPair.getT1()))
                        .map(t -> Tuples.of(resPair.getT1(), resPair.getT2(), t.getT2())))
                .flatMap(resTriad -> resTriad.getT2()
                        // Complete responseSink with success / error / empty
                        .doOnNext(v -> resTriad.getT3().success(v))
                        .doOnError(e -> resTriad.getT3().error(e))
                        .switchIfEmpty(Mono.just(resTriad.getT3())
                                .doOnNext(MonoSink::success)
                                .flatMap(whatever -> Mono.empty()))
                        // Switch back to request id and collect to set
                        .thenReturn(resTriad.getT1())
                        .onErrorReturn(resTriad.getT1()))
                .collect(Collectors.toSet())
                // Filter not replied ids and send 'unknown' results
                .flatMapMany(replied -> Flux.fromIterable(fuel.entrySet())
                        .filter(entry -> !replied.contains(entry.getKey()))
                        .map(entry -> entry.getValue().getT2())
                        .doOnNext(sink -> sink.error(new IllegalStateException("Unknown result"))));
    }
}
