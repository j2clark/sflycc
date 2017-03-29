package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventBuilder;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.EventVerb;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.UUID;

@Component
public class SiteVisitIngestProcessor extends AbstractIngestProcessor implements IngestProcessor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    public SiteVisitIngestProcessor(IngestProcessorRegistry ingestProcessorRegistry) {
        super(ingestProcessorRegistry,
              Collections.singletonList(new EventType("SITE_VISIT")),
              Collections.singletonList(new EventVerb("NEW")));
    }

    @Override
    public void adapt(UUID transactionId, JSONObject json, EventBuilder eventBuilder)
        throws UnsupportedEventException {

        eventBuilder.withAttribute("customer_id", findRequiredAttribute(transactionId, json, "customer_id")); // required
    }
}
