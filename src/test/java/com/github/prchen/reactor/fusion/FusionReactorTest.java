package com.github.prchen.reactor.fusion;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.LongStream;

/**
 * @author prchen
 */
class FusionReactorTest {
    private static FusionFactory factory;

    @BeforeAll
    static void beforeAll() {
        factory = new FusionFactory();
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        factory.shutdown(Duration.ofMillis(Long.MAX_VALUE));
    }

    @Test
    void testCommonUse() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(fuel -> Flux.fromIterable(fuel.entrySet())
                .groupBy(Map.Entry::getValue, Map.Entry::getKey)
                .flatMap(group -> group.map(id -> Tuples.of(id, Mono.just(String.valueOf(group.key()))))));
        // Submit requests
        Flux<Tuple2<Long, String>> res = Flux.fromStream(LongStream.range(0, 10000).boxed())
                .map(whatever -> ThreadLocalRandom.current().nextLong())
                .flatMap(input -> reactor.react(input)
                        .map(output -> Tuples.of(input, output)));
        // Verify result
        StepVerifier.create(res)
                .thenConsumeWhile(item -> Objects.equals(item.getT1().toString(), item.getT2()))
                .expectComplete()
                .verify();
    }

    @Test
    void testSimpleIdentified() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(FusionReaction.simpleIdentified(
                Function.identity(),
                values -> Flux.fromIterable(values)
                        .collectMap(Function.identity(), String::valueOf)));
        // Submit requests
        Flux<Tuple2<Long, String>> res = Flux.fromStream(LongStream.range(0, 10000).boxed())
                .map(whatever -> ThreadLocalRandom.current().nextLong())
                .flatMap(input -> reactor.react(input)
                        .map(output -> Tuples.of(input, output)));
        // Verify result
        StepVerifier.create(res)
                .thenConsumeWhile(item -> Objects.equals(item.getT1().toString(), item.getT2()))
                .expectComplete()
                .verify();
    }

    @Test
    void testSimplePartitioned() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(FusionReaction.simplePartitioned(
                value -> value % 10L,
                Function.identity(),
                values -> Flux.fromIterable(values)
                        .collectMap(Function.identity(), String::valueOf)));
        // Submit requests
        Flux<Tuple2<Long, String>> res = Flux.fromStream(LongStream.range(0, 10000).boxed())
                .map(whatever -> ThreadLocalRandom.current().nextLong())
                .flatMap(input -> reactor.react(input)
                        .map(output -> Tuples.of(input, output)));
        // Verify result
        StepVerifier.create(res)
                .thenConsumeWhile(item -> Objects.equals(item.getT1().toString(), item.getT2()))
                .expectComplete()
                .verify();
    }

    @Test
    void testEmptyLoop() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        // Create reactor instance
        AtomicBoolean invoked = new AtomicBoolean(false);
        FusionReactor<Long, String> reactor = factory.getInstance(fuel -> {
            invoked.set(true);
            return Flux.just();
        });
        // Trigger core loop
        Field field = reactor.getClass().getDeclaredField("semaphore");
        field.setAccessible(true);
        try {
            do {
                reactor.triggerLoop();
            } while (((Semaphore) field.get(reactor)).tryAcquire(50, TimeUnit.MILLISECONDS));
        } finally {
            field.setAccessible(false);
        }
        // Verify result
        Assertions.assertFalse(invoked.get());
    }

    @Test
    void testDirectlyThrow() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(fuel -> {
            throw new IllegalStateException("directly throw");
        });
        // Submit request
        Mono<String> res = reactor.react(ThreadLocalRandom.current().nextLong());
        // Verify result
        StepVerifier.create(res)
                .expectErrorMatches(e -> e instanceof IllegalStateException
                        && "directly throw".equals(e.getMessage())
                        && Objects.isNull(e.getCause()))
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void testEmptyReply() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(fuel -> Flux.fromIterable(fuel.keySet())
                .map(id -> Tuples.of(id, Mono.empty())));
        // Submit request
        Mono<String> res = reactor.react(ThreadLocalRandom.current().nextLong());
        // Verify result
        StepVerifier.create(res)
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void testDuplicateReply() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(fuel -> Flux.fromIterable(fuel.entrySet())
                .groupBy(Map.Entry::getValue, Map.Entry::getKey)
                .flatMap(group -> group.flatMap(id -> Flux.just(
                        Tuples.of(id, Mono.just(String.valueOf(group.key()))),
                        Tuples.of(id, Mono.just(String.valueOf(group.key())))))));
        // Submit request
        long value = ThreadLocalRandom.current().nextLong();
        Mono<String> res = reactor.react(value);
        // Verify result
        StepVerifier.create(res)
                .expectNext(String.valueOf(value))
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void testUnknownResult() {
        // Create reactor instance
        FusionReactor<Long, String> reactor = factory.getInstance(fuel -> Flux.just());
        // Submit request
        Mono<String> res = reactor.react(ThreadLocalRandom.current().nextLong());
        // Verify result
        StepVerifier.create(res)
                .expectErrorMatches(e -> e instanceof IllegalStateException
                        && "Unknown result".equals(e.getMessage())
                        && Objects.isNull(e.getCause()))
                .verify(Duration.ofSeconds(1));
    }
}