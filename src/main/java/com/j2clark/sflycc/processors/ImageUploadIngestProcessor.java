package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventBuilder;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.EventVerb;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.UUID;

@Component
public class ImageUploadIngestProcessor extends AbstractIngestProcessor implements IngestProcessor {

    @Autowired
    public ImageUploadIngestProcessor(IngestProcessorRegistry ingestProcessorRegistry) {
        super(ingestProcessorRegistry,
              Collections.singletonList(new EventType("IMAGE")),
              Collections.singletonList(new EventVerb("UPLOAD")));
    }

    @Override
    public void adapt(UUID transactionId, JSONObject json, EventBuilder eventBuilder)
        throws UnsupportedEventException {

        eventBuilder.withAttribute("customer_id", findRequiredAttribute(transactionId, json, "customer_id")); // required
        eventBuilder.withAttribute("camera_make", findAttribute(json, "camera_make"));
        eventBuilder.withAttribute("camera_model", findAttribute(json, "camera_model"));
    }
}
