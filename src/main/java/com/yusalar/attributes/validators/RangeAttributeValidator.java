package com.yusalar.attributes.validators;

/**
 * Validator for cases like &format=1-10
 */
public class RangeAttributeValidator<T extends Comparable> implements AttributeValidator<T> {
    private final T begin;
    private final T end;

    public RangeAttributeValidator(T begin, T end) {
        this.begin = begin;
        this.end = end;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean validate(T value) {
        return begin.compareTo(value) <= 0 && end.compareTo(value) >= 0;
    }
}
