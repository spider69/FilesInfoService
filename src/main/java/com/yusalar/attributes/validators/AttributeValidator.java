package com.yusalar.attributes.validators;

/**
 * Base interface for various validators of attributes
 */
public interface AttributeValidator<T> {
    boolean validate(T value);
}
