package com.j2clark.sflycc.domain;

import com.j2clark.sflycc.processors.UnsupportedEventException;

import org.joda.money.Money;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class EventBuilder {

    private EventType type;
    private EventVerb verb;
    private EventKey key;
    private Long timestamp;
    private Map<String,Event.Attribute> attributes = new HashMap<>();
    private Map<String,String> tags = new HashMap<>();

    public EventBuilder withType(EventType type) {
        this.type = type;
        return this;
    }

    public EventBuilder withVerb(EventVerb verb) {
        this.verb = verb;
        return this;
    }

    public EventBuilder withKey(String key) {
        if (!StringUtils.isEmpty(key)) {
            return withKey(EventKey.of(key));
        }
        return this;
    }

    public EventBuilder withKey(EventKey key) {
        if (key != null) {
            this.key = key;
        }
        return this;
    }

    public EventBuilder withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public EventBuilder withTimestamp(Date eventDateTime) {
        if (eventDateTime != null) {
            return withTimestamp(eventDateTime.getTime());
        }
        return this;
    }

    public EventBuilder withTag(String key, String value) {
        if (!StringUtils.isEmpty(key)) {
            this.tags.put(key, value);
        } else {
            // ignoring badly formed (null key) tag
        }
        return this;
    }

    public EventBuilder withTags(Map<String,String> tags) {
        if (tags != null) {
            this.tags.putAll(tags);
        }
        return this;
    }

    public EventBuilder withAttribute(String name, Money value) {
        return withAttribute(new Event.Attribute<>(name, value));
    }

    public EventBuilder withAttribute(String name, String value) {
        return withAttribute(new Event.Attribute<>(name, value));
    }

    public EventBuilder withAttribute(String name, Date value) {
        return withAttribute(new Event.Attribute<>(name, value));
    }

    public EventBuilder withAttribute(String name, Long value) {
        return withAttribute(new Event.Attribute<>(name, value));
    }

    public EventBuilder withAttribute(Event.Attribute attribute) {
        attributes.put(attribute.getKey(), attribute);
        return this;
    }

    public static Money parseMoney(String moneyStr) {
        Money money = null;
        if (!StringUtils.isEmpty(moneyStr)) {
            // NOTE: original sample data shows date format as 123.45 USD, which seems to be non-standard and very difficult to parse
            // I am making assumption we are using standard (joda) format USD 123.45
            // otherwise afaik a custom parser will need to be written - which would be very tine consuming and error prone
            money = Money.parse(moneyStr);
        }
        return money;
    }

    public static Date parseISODate(String dateStr) {
        Date d = null;
        if (!StringUtils.isEmpty(dateStr)) {
            DateTime dateTime = ISODateTimeFormat.dateTime().parseDateTime(dateStr);
            d = dateTime.toDate();
        }
        return d;
    }

    public Event build() throws UnsupportedEventException {
        if (type == null) {
            throw new UnsupportedEventException("event type cannot be null");
        }

        if (verb == null) {
            throw new UnsupportedEventException("event verb cannot be null");
        }

        if (key == null) {
            throw new UnsupportedEventException("event key cannot be null");
        }

        if (timestamp == null) {
            throw new UnsupportedEventException("event timestamp cannot be null");
        }
        return new EventImpl(type, verb, key, timestamp, attributes, tags);
    }

    private static class EventImpl implements Event {

        private final EventType type;
        private final EventVerb verb;
        private final EventKey key;
        private final long timestamp;
        private final Map<String,Event.Attribute> attributes;
        private final Map<String,String> tags;

        private EventImpl(EventType type, EventVerb verb, final EventKey key, long timestamp, Map<String, Attribute> attributes, Map<String,String> tags) {
            this.type = type;
            this.verb = verb;
            this.key = key;
            this.timestamp = timestamp;
            if (attributes != null) {
                this.attributes = Collections.unmodifiableMap(new HashMap<>(attributes));
            } else {
                this.attributes = Collections.emptyMap();
            }
            if (tags != null) {
                this.tags = Collections.unmodifiableMap(new HashMap<>(tags));
            } else {
                this.tags = Collections.emptyMap();
            }
        }

        @Override
        public EventType getType() {
            return type;
        }

        @Override
        public EventVerb getVerb() {
            return verb;
        }

        @Override
        public Map<String, Attribute> getAttributes() {
            return attributes;
        }

        @Override
        public Attribute getAttribute(String key) {
            return attributes.get(key);
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        @Override
        public EventKey getKey() {
            return key;
        }
    }
}
