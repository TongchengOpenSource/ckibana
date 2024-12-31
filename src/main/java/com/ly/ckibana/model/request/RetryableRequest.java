package com.ly.ckibana.model.request;

@FunctionalInterface
public interface RetryableRequest<T> {
    T execute() throws Exception;
}
