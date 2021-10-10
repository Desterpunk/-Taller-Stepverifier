package com.sofkau.StepVerifier.services;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ServicioTest {
    @Autowired
    Servicio servicio;
    @Test
    void testMono() {
        Mono<String> uno = servicio.buscarUno();
        StepVerifier.create(uno).expectNext("Pedro").verifyComplete();
    }
    @Test
    void testVarios() {
        Flux<String> uno = servicio.buscarTodos();
        StepVerifier.create(uno).expectNext("Pedro").expectNext("Maria").expectNext("Jesus").expectNext("Carmen").verifyComplete();
    }

    @Test
    void testVariosLento() {
        Flux<String> uno = servicio.buscarTodosLento();
        StepVerifier.create(uno)
                .expectNext("Pedro")
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("Maria")
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("Jesus")
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("Carmen")
                .thenAwait(Duration.ofSeconds(1)).verifyComplete();
    }

//    @Test
//    void testTodosFiltro() {
//        Flux<String> source = servicio.buscarTodosFiltro();
//        StepVerifier
//                .create(source)
//                .expectNext("JOHN")
//                .expectNextMatches(name -> name.startsWith("MA"))
//                .expectNext("CLOE", "CATE")
//                .expectComplete()
//                .verify();
//    }

//    @Test
//    void testTodosFiltro() {
//        Flux<String> source = servicio.buscarTodosFiltro();
//        StepVerifier
//                .create(source)
//                .expectNextCount(4)
//                .expectErrorMatches(throwable -> throwable instanceof IllegalArgumentException &&
//                        throwable.getMessage().equals("Mensaje de Error")
//                ).verify();
//    }

    @Test
    void testPublicadorBasadoEnElTiempo() {
        StepVerifier
                .withVirtualTime(() -> Flux.interval(Duration.ofSeconds(1)).take(2))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(1))
                .expectNext(0L)
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(1L)
                .verifyComplete();
    }

    @Test
    void posteriorEjecucion() {
        Flux<Integer> source = servicio.posteriorEjecucion();
        StepVerifier.create(source)
                .expectNext(2)
                .expectComplete()
                .verifyThenAssertThat()
                .hasDropped(4)
                .tookLessThan(Duration.ofMillis(1050));
    }

    @Test
    void testPublisher() {
        TestPublisher
                .<String>create()
                .next("Primero", "Segundo", "Tercero")
                .error(new RuntimeException("Message"));
    }

}