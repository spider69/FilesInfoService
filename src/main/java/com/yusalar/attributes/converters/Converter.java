package com.yusalar.attributes.converters;

/**
 * Convert value of attribute from string representation to Comparable object.
 * Proper conversion depends on actual type of attribute.
 */
@FunctionalInterface
public interface Converter {
    Comparable convert(String attrValue);
}
