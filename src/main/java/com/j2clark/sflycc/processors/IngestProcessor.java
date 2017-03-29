package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.Event;

import org.json.JSONObject;

import java.util.Set;
import java.util.UUID;

public interface IngestProcessor {

    Set<EventType> getSupportedEventTypes();

    // we will want to wrap abstract JSONObject into an interface or abstract class, so we can consume various formats if needed
    Event adapt(UUID transactionId, JSONObject jsonObject) throws UnsupportedEventException;

}
