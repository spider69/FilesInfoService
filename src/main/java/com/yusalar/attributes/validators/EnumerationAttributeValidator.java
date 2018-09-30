package com.yusalar.attributes.validators;

import java.util.HashSet;
import java.util.Set;

/**
 * Validator for cases like &size=4,5,8,7 or &size=8
 */
public class EnumerationAttributeValidator<T> implements AttributeValidator<T> {
    private final Set<T> enumeration;

    public EnumerationAttributeValidator(T value) {
        enumeration = new HashSet<>();
        enumeration.add(value);
    }

    public EnumerationAttributeValidator(Set<T> enumeration) {
        this.enumeration = enumeration;
    }

    @Override
    public boolean validate(T value) {
        return enumeration.contains(value);
    }
}
