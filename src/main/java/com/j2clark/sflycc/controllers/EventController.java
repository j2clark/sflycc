package com.j2clark.sflycc.controllers;

import com.j2clark.sflycc.domain.Data;
import com.j2clark.sflycc.services.IngestService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.UUID;

@Controller
@RequestMapping(value = "/event", consumes = "application/json", produces = "application/json")
public class EventController {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Logger eventLogger = LoggerFactory.getLogger("events");

    private final IngestService ingestService;

    @Autowired
    public EventController(final IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public @ResponseBody Data pushEvent(final @RequestBody String rawRequestBody) {

        // add request client details, so we know who published event

        // transaction id for event log
        // ideally we pass the transaction id downstream for use in log
        UUID transactionId = UUID.randomUUID();

        eventLogger.info("RAW EVENT["+transactionId+"]: " + rawRequestBody);

        return ingestService.ingest(transactionId, rawRequestBody);
    }

}
