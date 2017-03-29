package com.j2clark.sflycc.processors;

import com.j2clark.sflycc.domain.EventType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * IngestProcessorRegistry is a central repository of all active EventProcessors in the application
 *
 * It relies on a processor registering itself upon instantiation
 * See CustomerIngestProcessor constructor for an example
 */
@Component
public class IngestProcessorRegistry {

    public final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<EventType,IngestProcessor>
        ingestProcessors = new HashMap<EventType, IngestProcessor>();
    public IngestProcessorRegistry register(IngestProcessor ingestProcessor) {
        // a single processor can support multiple event types

        String processorName = ingestProcessor.getClass().getSimpleName();
        if (logger.isInfoEnabled()) {
            logger.info(
                "Registering IngestProcessor["+processorName+"]");
        }

        for (EventType eventType : ingestProcessor.getSupportedEventTypes()) {
            if (!ingestProcessors.containsKey(eventType)) {
                ingestProcessors.put(eventType, ingestProcessor);
                if (logger.isInfoEnabled()) {
                    logger.info(
                        "IngestProcessor["+processorName+"] registered to handle EventType["+eventType+"]");
                }
            } else {
                // DO NOT let the application start up if we have a configuration problem
                throw new IllegalStateException("Invalid Configuration, ingestProcessor of type["+eventType+"] already registered.");
            }
        }

        return this;
    }

    /**
     * @param type of event we are trying to process
     * @return IngestProcessor instance if we find one
     */
    public Optional<IngestProcessor> find(EventType type) {
        if (ingestProcessors.containsKey(type)) {
            return Optional.of(ingestProcessors.get(type));
        } else {
            return Optional.empty();
        }
    }
}
