package com.github.prchen.reactor.fusion;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * @param <T> Request type
 * @param <U> Response type
 * @author prchen
 */
@FunctionalInterface
public interface FusionReaction<T, U> {
    /**
     * Batch process a couple of requests and reply a stream that carries responses
     *
     * @param fuel Aggregated requests identified by an integer
     * @return Stream of response attached with the given integer key passed from `fuel` parameter
     */
    Flux<Tuple2<Integer, Mono<U>>> react(Map<Integer, T> fuel);

    /**
     * Associate request and response by an identifier that cloud be mapped from request object
     *
     * @param identifiedBy Identifying function
     * @param react        Business function
     * @param <T>          Request type
     * @param <U>          Response type
     * @param <K>          Identifying key type
     * @return FusionReaction
     */
    static <T, U, K> FusionReaction<T, U> simpleIdentified(
            Function<T, K> identifiedBy, Function<Collection<T>, Mono<Map<K, U>>> react) {
        return fuel -> Mono.just(fuel.values())
                .flatMap(react)
                .flatMapMany(ret -> Flux.fromIterable(fuel.entrySet())
                        .groupBy(entry -> identifiedBy.apply(entry.getValue()), Map.Entry::getKey, fuel.size())
                        .flatMap(group -> group.map(id -> Tuples.of(id, Mono.justOrEmpty(ret.get(group.key()))))));
    }

    /**
     * Partition requests into several partitions and make parallel execution
     *
     * @param partitionedBy Partitioning function
     * @param identifiedBy  Identifying function
     * @param react         Business function
     * @param <T>           Request type
     * @param <U>           Response type
     * @param <P>           Partitioning key type
     * @param <K>           Identifying key type
     * @return FusionReaction
     */
    static <T, U, P, K> FusionReaction<T, U> simplePartitioned(
            Function<T, P> partitionedBy, Function<T, K> identifiedBy, Function<Collection<T>, Mono<Map<K, U>>> react) {
        return fuel -> Flux.fromIterable(fuel.values())
                .groupBy(partitionedBy, Function.identity(), fuel.size())
                .parallel(Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE)
                .runOn(Schedulers.boundedElastic())
                .flatMap(Flux::collectList)
                .flatMap(react)
                .sequential()
                .flatMap(ret -> Flux.fromIterable(fuel.entrySet())
                        .filter(entry -> ret.containsKey(identifiedBy.apply(entry.getValue())))
                        .groupBy(entry -> identifiedBy.apply(entry.getValue()), Map.Entry::getKey, fuel.size())
                        .flatMap(group -> group.map(id -> Tuples.of(id, Mono.justOrEmpty(ret.get(group.key()))))));
    }
}
