package com.yusalar.attributes;

import com.yusalar.attributes.converters.Converter;
import com.yusalar.attributes.validators.AttributeValidator;
import com.yusalar.attributes.validators.EnumerationAttributeValidator;
import com.yusalar.attributes.validators.RangeAttributeValidator;

import java.util.*;
import java.util.stream.Collectors;

public class AttributeValidatorsFactory {
    private static AttributeValidatorsFactory INSTANCE;
    private Map<String, Converter> registeredAttrs = new HashMap<>();

    private AttributeValidatorsFactory() {}

    public static AttributeValidatorsFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new AttributeValidatorsFactory();
        }
        return INSTANCE;
    }

    /**
     * Method for registration of attributes
     * @param name of attribute
     * @param converter for converting string to proper type of attribute
     */
    public void registerAttribute(String name, Converter converter) {
        registeredAttrs.put(name, converter);
    }

    /**
     * Method for getting proper AttributeValidator for stringAttr
     * @param name of attribute
     * @param stringAttr represents selected attributes (example: &zone=1,3,6 -> stringAttr=1,3,6)
     */
    @SuppressWarnings("unchecked")
    public AttributeValidator parseAttributeFromString(String name, String stringAttr) throws IllegalArgumentException {
        if (!registeredAttrs.containsKey(name)) throw new IllegalArgumentException("Unregistered attribute name: " + name);

        Converter converter = registeredAttrs.get(name);
        // TODO: add errors handling while parsing
        if (stringAttr.contains(",")) {
            String[] attrsEnum = stringAttr.split(",");
            return new EnumerationAttributeValidator(Arrays.stream(attrsEnum)
                    .map(converter::convert)
                    .collect(Collectors.toSet()));
        } else if (stringAttr.contains("-")) {
            String[] attrsRange = stringAttr.split("-");
            if (attrsRange.length != 2) throw new IllegalArgumentException("Wrong format of range");
            Comparable begin = converter.convert(attrsRange[0]);
            Comparable end = converter.convert(attrsRange[1]);
            if (begin.compareTo(end) > 0) throw new IllegalArgumentException("Begin of range is greater than end");
            return new RangeAttributeValidator(begin, end);
        } else {
            Comparable singleValue = converter.convert(stringAttr);
            return new EnumerationAttributeValidator(singleValue);
        }
    }
}
