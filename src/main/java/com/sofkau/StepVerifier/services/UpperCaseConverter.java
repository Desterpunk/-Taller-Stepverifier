package com.sofkau.StepVerifier.services;

import reactor.core.publisher.Flux;

public class UpperCaseConverter {
    private final Flux<String> source;
    UpperCaseConverter(Flux<String> source) {
        this.source = source;
    }
    Flux<String> getUpperCase() {
        return source
                .map(String::toUpperCase);
    }
}
