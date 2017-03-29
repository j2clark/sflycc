package com.j2clark.sflycc.domain;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

/**
 * A simple DTO for the time being
 */
public class Data {

    private final UUID transactionId;
    private final Set<Event> events = new LinkedHashSet<>();

    public Data(final UUID transactionId) {
        this.transactionId = transactionId;
    }

    public Data append(Event event)
    {
        events.add(event);

        return this;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public Collection<Event> getEvents() {
        return events;
    }


}
