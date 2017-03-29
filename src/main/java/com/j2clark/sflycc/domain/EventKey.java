package com.j2clark.sflycc.domain;

import org.springframework.util.StringUtils;

public class EventKey {

    public static EventKey of(String type) {
        return new EventKey(type);
    }

    private final String key;

    public EventKey(String key) {
        if (!StringUtils.isEmpty(key)) {
            // normalize to uppercase
            this.key = key.trim();
        } else {
            throw new IllegalArgumentException("EventKey key cannot be empty");
        }
    }

    public String getValue() {
        return key;
    }

    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventKey)) {
            return false;
        }

        EventKey eventKey = (EventKey) o;

        return key.equals(eventKey.key);

    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
