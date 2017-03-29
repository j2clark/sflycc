package com.j2clark.sflycc.domain;

import java.util.Map;

public interface Event {

    EventType getType();
    EventVerb getVerb();
    EventKey getKey();        // always present
    long getTimestamp();    // always present
    Map<String,Attribute> getAttributes();
    Attribute getAttribute(String key);
    Map<String,String> getTags();

    class Attribute<T> {
        private final String key;
        private final T value;

        public Attribute(String key, T value) {
            this.key = key;
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        public String getKey() {
            return key;
        }
    }
}
