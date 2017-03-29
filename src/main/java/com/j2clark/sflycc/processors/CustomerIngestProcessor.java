package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventBuilder;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.EventVerb;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

@Component
public class CustomerIngestProcessor extends AbstractIngestProcessor implements IngestProcessor {

    @Autowired
    public CustomerIngestProcessor(IngestProcessorRegistry ingestProcessorRegistry) {
        super(ingestProcessorRegistry,
              Collections.singletonList(new EventType("CUSTOMER")),
              Arrays.asList(
                  new EventVerb("NEW"),
                  new EventVerb("UPDATE")
              ));
    }

    @Override
    public void adapt(UUID transactionId, JSONObject json, EventBuilder eventBuilder) throws UnsupportedEventException {
        // optional data
        eventBuilder.withAttribute("last_name", findAttribute(json, "last_name"));
        eventBuilder.withAttribute("adr_city", findAttribute(json, "adr_city"));
        eventBuilder.withAttribute("adr_state", findAttribute(json, "adr_state"));
    }
}
