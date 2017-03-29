package com.j2clark.sflycc.services;

import com.j2clark.sflycc.domain.Data;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.Event;
import com.j2clark.sflycc.processors.IngestProcessor;
import com.j2clark.sflycc.processors.IngestProcessorRegistry;
import com.j2clark.sflycc.processors.UnsupportedEventException;

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

/**
 * primary service for receiving, processing, and validating incoming events
 *
 * Very simplistic approach, assuming raw json data rather than something a little more cleanly abstracted
 * I decided to keep a simple Event object instead of defining more precise event classes
 * Also there seems to be a lot of opportunity to leverage java 8 streams here (and thorough app),
 * especially if we want to leverage parallelism - there are several places we could easily leverage that
 */
@Service
public class IngestService {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Logger payloadLogger = LoggerFactory.getLogger("payload");


    private final IngestProcessorRegistry ingestProcessorRegistry;

    @Autowired
    public IngestService(final IngestProcessorRegistry ingestProcessorRegistry) {
        this.ingestProcessorRegistry = ingestProcessorRegistry;
    }

    /**
     * Auto generate Data instance, ingest json event data and add to data
     */
    public Data ingest(UUID transactionId, String json) {
        Data dataRepository = new Data(transactionId);

        ingest(dataRepository, json);

        return dataRepository;
    }

    /**
     * This is an orchestration method which results in:
     *
     *      process(
     *          parseEvents(dataRepository, json),
     *          dataRepository
     *      )
     *
     * Given a json event string (are we size constrained using this approach?)
     * parse each event entry and add to Data repository
     * Events which are not supported, or badly formatted, will be logged and ignored
     * A good enhancement would be to track all errors and add them to the data object
     */
    public void ingest(Data dataRepository, String json) {

        // parse and order
        Collection<Event> events = parseEvents(dataRepository.getTransactionId(), json);

        // process
        process(events, dataRepository);
    }

    /**
     * Simply adds events to data repository for now
     * I was thinking we could do more advanced validation here, but a lack of time prevents me from exploring
     */
    public void process(Collection<Event> events, Data dataRepository) {

        // for now I am skipping validating the existence of dependencies,
        // e.g. I am guessing SITE_VISIT should fail in the given input data, since it depends on a customer that has not yet been created
        // although this begs the question about timestamps, and how much we can/should depend on their accuracy

        for(Event event : events) {

            dataRepository.append(event);

        }

    }

    /**
     * Given a json event string (are we size constrained using this approach?)
     * parse each event using an ingestProcessor, found using the eventType
     * Events which are not supported, or badly formatted, will be logged and ignored
     * A good enhancement would be to track all errors and add them to the data object
     */
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
                            } catch (UnsupportedEventException e) {
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

    /**
     * parse a given json event using an ingestProcessor, found using the eventType
     * If the event is not supported, or badly formatted, we will throw an UnsupportedEventException
     * We could refine our exception model in the future buy being more explicit as to why it is unsupported
     */
    protected Event onEventObject(UUID transactionId, JSONObject jsonObject) throws
                                                                             UnsupportedEventException {
        Optional<IngestProcessor> processor = findProcessor(jsonObject);
        if (processor.isPresent()) {
            try {
                // we will either get an event, or an exception will be thrown
                return processor.get().adapt(transactionId, jsonObject);
            } catch (Throwable e) {
                // we should only ever receive UnsupportedEventException...
                // but we will protect ourselves from breaking due to unexpected exceptions
                logger.error("Request["+transactionId+"]", e);
                throw new UnsupportedEventException(e);
            }
        } else {
            throw new UnsupportedEventException("No processor found");

        }

        //return event;
    }

    /**
     * locate and return an IngestProcessor for given eventType
     */
    protected Optional<IngestProcessor> findProcessor(JSONObject jsonObject) throws
                                                                             UnsupportedEventException {
        try {
            EventType type = EventType.of(jsonObject.getString("type"));
            return ingestProcessorRegistry.find(type);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedEventException(e);
        }
    }

}
