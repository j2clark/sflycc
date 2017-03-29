package com.j2clark.sflycc.services;

import com.j2clark.sflycc.domain.Data;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.Event;
import com.j2clark.sflycc.processors.IngestProcessor;
import com.j2clark.sflycc.processors.IngestProcessorRegistry;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class IngestService {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Logger payloadLogger = LoggerFactory.getLogger("payload");


    private final IngestProcessorRegistry ingestProcessorRegistry;

    @Autowired
    public IngestService(final IngestProcessorRegistry ingestProcessorRegistry) {
        this.ingestProcessorRegistry = ingestProcessorRegistry;
    }

    public Data ingest(UUID transactionId, String json) {
        Data dataRepository = new Data(transactionId);

        ingest(transactionId, dataRepository, json);

        return dataRepository;
    }


    public void ingest(UUID transactionId, Data dataRepository, String json) {

        // parse and order
        Collection<Event> events = parseEvents(transactionId, json);

        // process
        process(transactionId, events, dataRepository);
    }

    public void process(UUID transactionId, Collection<Event> events, Data dataRepository) {

        // re-order events according to timestamp, in case this was called externally
        Collection<Event> sorted = events.stream()
            .sorted((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()))
            .collect(Collectors.toList());

        // todo: for now I am skipping validating the existence of dependencies,
        // e.g. SITE_VISIT should fail, since it depends on a customer that has not yet been created
        // although this begs the question about timestamps, and how much we can/should depend on their accuracy

        for(Event event : sorted) {

            dataRepository.append(event);

        }

    }

    public Collection<Event> parseEvents(UUID transactionId, String json) {

        if (!StringUtils.isEmpty(json)) {
            payloadLogger.info("Request["+transactionId+"] " + json);

            // begin transformation/validation pipeline

            // process models into events
            Collection<Event> events = new LinkedHashSet<>();
            if (json.trim().startsWith("[")) {
                try {
                    JSONArray jsonArray = new JSONArray(json);

                    for (Object o : jsonArray) {
                        if (o instanceof JSONObject) {
                            try {
                                events.add(onEventObject(transactionId, (JSONObject) o));
                            } catch (IngestProcessor.UnsupportedEventException e) {
                                // todo: log and move on to next
                            }
                        } else {
                            // unsupported data structure - log and continue
                            logger.error("expecting JSONObject, instead found["+o.getClass().getName()+"]");
                        }

                    }
                } catch (Throwable t) {
                    // log exception and continue
                    logger.error("", t);
                }
            } else {
                try {
                    events.add(onEventObject(transactionId, new JSONObject(json)));
                } catch (Throwable t) {
                    // log exception and continue
                    logger.error("", t);
                }
            }


            // order events according to timestamp
            events = events.stream()
                .sorted((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()))
                .collect(Collectors.toList());

            // return results
            logger.info("Request["+transactionId+"] resulted in["+events.size()+"] events");

            return events;
        } else {
            return Collections.emptyList();
        }

    }

    protected Event onEventObject(UUID transactionId, JSONObject jsonObject) throws IngestProcessor.UnsupportedEventException {
        Optional<IngestProcessor> processor = findProcessor(transactionId, jsonObject);
        if (processor.isPresent()) {
            try {
                // we will either get an event, or an exception will be thrown
                return processor.get().adapt(transactionId, jsonObject);
            } catch (Throwable e) {
                // we should obnly ever receive UnsupportedEventException...
                // but we will protect ourselves from breaking due to unexpected exceptions
                logger.error("Request["+transactionId+"]", e);
                throw new IngestProcessor.UnsupportedEventException(e);
            }
        } else {
            throw new IngestProcessor.UnsupportedEventException("No processor found");

        }

        //return event;
    }

    protected Optional<IngestProcessor> findProcessor(UUID transactionId, JSONObject jsonObject) throws IngestProcessor.UnsupportedEventException {
        try {
            EventType type = EventType.of(jsonObject.getString("type"));
            return ingestProcessorRegistry.find(type);
        } catch (IllegalArgumentException e) {
            throw new IngestProcessor.UnsupportedEventException(e);
        }
    }

}
