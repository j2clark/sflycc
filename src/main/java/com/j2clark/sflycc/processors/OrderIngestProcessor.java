package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventBuilder;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.EventVerb;

import org.joda.money.Money;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

@Component
public class OrderIngestProcessor extends AbstractIngestProcessor implements IngestProcessor {

    @Autowired
    public OrderIngestProcessor(
        IngestProcessorRegistry ingestProcessorRegistry) {
        super(ingestProcessorRegistry,
              Collections.singletonList(new EventType("order")),
              Arrays.asList(
                  new EventVerb("NEW"),
                  new EventVerb("UPDATE")
              ));
    }

    @Override
    public void adapt(UUID transactionId, JSONObject json, EventBuilder eventBuilder)
        throws UnsupportedEventException {

        eventBuilder.withAttribute("customer_id", findRequiredAttribute(transactionId, json, "customer_id"));

        // I am assuming a different money format than was listed in the sample input
        Money orderTotal = EventBuilder.parseMoney(
            findRequiredAttribute(transactionId, json, "total_amount")
        );
        eventBuilder.withAttribute("total_amount", orderTotal);

    }
}
