package com.sofkau.StepVerifier.services;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.junit.jupiter.api.Assertions.*;

class UpperCaseConverterTest {
    final TestPublisher<String> testPublisher = TestPublisher.create();
    @Test
    void testUpperCase() {
        UpperCaseConverter uppercaseConverter = new UpperCaseConverter(testPublisher.flux());
        StepVerifier.create(uppercaseConverter.getUpperCase())
                .then(() -> testPublisher.emit("datos", "GeNeRaDoS", "Sofka"))
                .expectNext("DATOS", "GENERADOS", "SOFKA")
                .verifyComplete();
    }

    @Test
    void casoInesperado() {
        TestPublisher
                .createNoncompliant(TestPublisher.Violation.ALLOW_NULL)
                .emit("1", "2", null, "3");
    }

}