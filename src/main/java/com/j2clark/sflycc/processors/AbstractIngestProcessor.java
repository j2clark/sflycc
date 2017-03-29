package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventBuilder;
import com.j2clark.sflycc.domain.EventKey;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.EventVerb;
import com.j2clark.sflycc.domain.Event;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public abstract class AbstractIngestProcessor implements IngestProcessor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Set<EventType> supportedTypes;
    private final Set<EventVerb> supportedVerbs;

    /**
     * Append event specific data into event using existing builder
     *
     * BE a good citizen and catch all exceptions and convert to UnsupportedEventException or subclass
     */
    protected abstract void adapt(UUID transactionId, JSONObject json, EventBuilder eventBuilder)
        throws UnsupportedEventException;

    @Autowired
    public AbstractIngestProcessor(IngestProcessorRegistry ingestProcessorRegistry,
                                   Collection<EventType> supportedTypes,
                                   Collection<EventVerb> supportedVerbs) {
        if (supportedTypes != null && !supportedTypes.isEmpty()) {
            this.supportedTypes = Collections.unmodifiableSet(new LinkedHashSet<>(supportedTypes));
        } else {
            throw new IllegalStateException(
                "Invalid Configuration - at least one supportedType required for " + getClass()
                    .getSimpleName());
        }

        if (supportedVerbs != null && !supportedVerbs.isEmpty()) {
            this.supportedVerbs = Collections.unmodifiableSet(new LinkedHashSet<>(supportedVerbs));
        } else {
            throw new IllegalStateException(
                "Invalid Configuration - at least one supportedVerb required for " + getClass()
                    .getSimpleName());
        }

        ingestProcessorRegistry.register(this);
    }

    @Override
    public Set<EventType> getSupportedEventTypes() {
        return supportedTypes;
    }


    @Override
    public Event adapt(UUID transactionId, JSONObject json) throws UnsupportedEventException {
        // do we support this verb?
        EventVerb verb = verb(json);
        EventType type = type(json);
        EventKey key = key(json);
        Date eventDateTime = eventDateTime(json);
        Map<String,String> tags = tags(transactionId, json);


        if (!supportedVerbs.contains(verb)) {
            throw new UnsupportedEventException("Request[" + transactionId + "] EventVerb[" + verb
                                                + "] not supported for EventType[" + type + "]");
        }

        // required data for all events
        EventBuilder eventBuilder = new EventBuilder();
        eventBuilder.withType(type);
        eventBuilder.withVerb(verb);
        eventBuilder.withKey(key);  // required
        eventBuilder.withTimestamp(eventDateTime); // required
        eventBuilder.withTags(tags);

        adapt(transactionId, json, eventBuilder);

        return eventBuilder.build();
    }

    protected Map<String,String> tags(UUID transactionId, JSONObject json) {
        Map<String,String> tagMap = new HashMap<>();
        if (json.has("tags")) {
            JSONArray tags = json.getJSONArray("tags");
            if (tags != null && tags.length() > 0) {
                // todo: how do we capture these?
                for (Object o : tags) {
                    if (o instanceof JSONObject) {
                        // I am making an assumption tags are a key value pair, anything else will be rejected
                        JSONObject jsonTag = (JSONObject) o;
                        Iterator<String> keys = jsonTag.keys();
                        while (keys.hasNext()) {
                            String key = keys.next();
                            String value = jsonTag.getString(key);
                            if (tagMap.containsKey(key)) {
                                logger.warn("Request["+transactionId+"] Duplicate tag keys found for key[" + key
                                            + "]. We are keeping value[" + tagMap.get(key)
                                            + "], ignoring[" + value + "]");
                            } else {
                                tagMap.put(key, value);
                            }
                        }
                    } else {
                        // ignore and move on
                        // todo: log at debug level?
                    }
                }
            }
        }
        return tagMap;
    }

    protected EventType type(JSONObject json) {
        if (!StringUtils.isEmpty(json.getString("type"))) {
            return EventType.of(json.getString("type"));
        }

        return null;
    }

    protected EventVerb verb(JSONObject json) {
        if (!StringUtils.isEmpty(json.getString("verb"))) {
            return EventVerb.of(json.getString("verb"));
        }

        return null;
    }

    protected EventKey key(JSONObject json) {
        if (!StringUtils.isEmpty(json.getString("key"))) {
            return EventKey.of(json.getString("key"));
        }

        return null;
    }

    protected Date eventDateTime(JSONObject json) {
        return EventBuilder.parseISODate(json.getString("event_time"));
    }


    protected String findAttribute(JSONObject json, String key) {
        if (json.has(key)) {
            return json.getString(key);
        } else {
            return null;
        }
    }

    protected String findRequiredAttribute(UUID transactionId, JSONObject json, String key) throws UnsupportedEventException {
        String value = findAttribute(json, key);
        if (StringUtils.isEmpty(value)) {
            throw new UnsupportedEventException(
                "Request[" + transactionId + "] EventVerb[" + verb(json) + "] EventType["
                + type(json) + "] "+key+" cannot be empty");
        }
        return value;
    }

}
