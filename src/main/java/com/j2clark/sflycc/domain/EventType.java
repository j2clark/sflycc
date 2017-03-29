package com.j2clark.sflycc.domain;

import org.springframework.util.StringUtils;

public class EventType {

    public static EventType of(String type) {
        return new EventType(type);
    }

    private final String type;

    public EventType(String type) {
        if (!StringUtils.isEmpty(type)) {
            // normalize to uppercase
            this.type = type.trim().toUpperCase();
        } else {
            throw new IllegalArgumentException("EventType name cannot be empty");
        }
    }

    public String getValue() {
        return type;
    }

    public String toString() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventType)) {
            return false;
        }

        EventType type1 = (EventType) o;

        return type.equals(type1.type);

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }
}
