package com.j2clark.sflycc.domain;

import org.springframework.util.StringUtils;

public class EventVerb {

    public static final EventVerb NEW = new EventVerb("NEW");
    public static final EventVerb UPDATE = new EventVerb("UPDATE");
    public static final EventVerb DELETE = new EventVerb("DELETE");

    public static EventVerb of(String verb) {
        return new EventVerb(verb);
    }

    private final String verb;

    public EventVerb(String verb) {
        if (!StringUtils.isEmpty(verb)) {
            // normalize to uppercase
            this.verb = verb.trim().toUpperCase();
        } else {
            throw new IllegalArgumentException("EventVerb name cannot be empty");
        }
    }

    public String getValue() {
        return verb;
    }

    public String toString() {
        return verb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventVerb)) {
            return false;
        }

        EventVerb eventVerb = (EventVerb) o;

        return !(verb != null ? !verb.equals(eventVerb.verb) : eventVerb.verb != null);

    }

    @Override
    public int hashCode() {
        return verb != null ? verb.hashCode() : 0;
    }
}
