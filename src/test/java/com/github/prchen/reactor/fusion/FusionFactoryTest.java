package com.github.prchen.reactor.fusion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author chenpeiran
 */
class FusionFactoryTest {

    @Test
    void testLifeCycle() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        // Build factory
        FusionFactory factory = new FusionFactory();
        // Create instances
        factory.getInstance(fuel -> Flux.just());
        factory.getInstance(fuel -> Flux.just());
        factory.getInstance(fuel -> Flux.just());
        Field reactorsField = factory.getClass().getDeclaredField("reactors");
        reactorsField.setAccessible(true);
        try {
            Assertions.assertEquals(3, ((List<?>) reactorsField.get(factory)).size());
        } finally {
            reactorsField.setAccessible(false);
        }
        // Shutdown
        boolean shutdown = factory.shutdown(Duration.ofMillis(Long.MAX_VALUE));
        Assertions.assertTrue(shutdown);
    }

    @Test
    void testShutdownNow() throws NoSuchFieldException, IllegalAccessException {
        // Build factory
        FusionFactory factory = new FusionFactory();
        // Create instances
        factory.getInstance(fuel -> Flux.just());
        factory.getInstance(fuel -> Flux.just());
        factory.getInstance(fuel -> Flux.just());
        // Shutdown now
        factory.shutdownNow();
        Field execField = factory.getClass().getDeclaredField("exec");
        execField.setAccessible(true);
        try {
            Assertions.assertTrue(((ExecutorService) execField.get(factory)).isShutdown());
        } finally {
            execField.setAccessible(false);
        }
    }
}