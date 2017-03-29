package com.j2clark.sflycc;

import com.j2clark.sflycc.domain.EventKey;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.EventVerb;
import com.j2clark.sflycc.domain.Event;
import com.j2clark.sflycc.processors.IngestProcessorRegistry;
import com.j2clark.sflycc.services.IngestService;

import org.joda.money.Money;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class IngestServiceTest extends AbstractServiceTest {

    //{"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
    @Test
    public void testNewSimpleCustomer() throws IOException {
        IngestService service = new IngestService(
            withCustomerProcessor(new IngestProcessorRegistry())
        );

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), jsonCustomer);
        assertEquals(1, events.size());

        Event event = events.stream().findFirst().get();
        assertEquals(EventType.of("CUSTOMER"), event.getType());
        assertEquals(EventVerb.of("NEW"), event.getVerb());
        assertEquals(EventKey.of("96f55c7d8f42"), event.getKey());
        assertEquals(newUTCDate(2017, 0, 6, 12, 46, 46, 384), new Date(event.getTimestamp()));
        assertEquals("Smith", event.getAttribute("last_name").getValue());
        assertEquals("Middletown", event.getAttribute("adr_city").getValue());
        assertEquals("AK", event.getAttribute("adr_state").getValue());
        assertTrue(event.getTags().isEmpty());
    }

    //{"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-06T12:46:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"}
    @Test
    public void testSimpleCustomer_NoState() throws IOException {
        IngestService service = new IngestService(
            withCustomerProcessor(new IngestProcessorRegistry())
        );

        String json = "{\"type\": \"CUSTOMER\", \"verb\": \"NEW\", \"key\": \"96f55c7d8f42\", \"event_time\": \"2017-01-06T12:46:46.384Z\", \"last_name\": \"Smith\", \"adr_city\": \"Middletown\"}";

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), json);
        assertEquals(1, events.size());

        Event event = events.stream().findFirst().get();
        assertEquals(EventType.of("CUSTOMER"), event.getType());
        assertEquals(EventVerb.of("NEW"), event.getVerb());
        assertEquals(EventKey.of("96f55c7d8f42"), event.getKey());
        assertEquals(newUTCDate(2017, 0, 6, 12, 46, 46, 384), new Date(event.getTimestamp()));
        assertEquals("Smith", event.getAttribute("last_name").getValue());
        assertEquals("Middletown", event.getAttribute("adr_city").getValue());
        Assert.assertNull(event.getAttribute("adr_state").getValue());
        assertTrue(event.getTags().isEmpty());
    }

    // {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06T12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": [{"some key": "some value"}]}
    @Test
    public void testSimpleSiteVisit() throws IOException {
        IngestService service = new IngestService(
            withSiteVisitProcessor(new IngestProcessorRegistry())
        );

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), jsonSiteVisit);
        assertEquals(1, events.size());

        Event event = events.stream().findFirst().get();
        assertEquals(EventType.of("SITE_VISIT"), event.getType());
        assertEquals(EventVerb.of("NEW"), event.getVerb());
        assertEquals(EventKey.of("ac05e815502f"), event.getKey());
        assertEquals(newUTCDate(2017, 0, 6, 12, 45, 52, 41), new Date(event.getTimestamp()));
        assertEquals("96f55c7d8f42", event.getAttribute("customer_id").getValue());
        assertFalse(event.getTags().isEmpty());
        assertEquals(1, event.getTags().size());
        assertTrue(event.getTags().containsKey("some key"));
        assertEquals("some value", event.getTags().get("some key"));
    }

    @Test
    public void testSiteVisit_NoCustomerId() throws IOException {
        IngestService service = new IngestService(
            withSiteVisitProcessor(new IngestProcessorRegistry())
        );

        String json = " {\"type\": \"SITE_VISIT\", \"verb\": \"NEW\", \"key\": \"ac05e815502f\", \"event_time\": \"2017-01-06T12:45:52.041Z\", \"tags\": [{\"some key\": \"some value\"}]}";

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), json);
        assertEquals(0, events.size());
    }

    //{"type": "IMAGE", "verb": "UPLOAD", "key": "d8ede43b1d9f", "event_time": "2017-01-06T12:47:12.344Z", "customer_id": "96f55c7d8f42", "camera_make": "Canon", "camera_model": "EOS 80D"}
    @Test
    public void testSimpleImageUpload() throws IOException {
        IngestService service = new IngestService(
            withImageUploadProcessor(new IngestProcessorRegistry())
        );

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), jsonImageUpload);
        assertEquals(1, events.size());

        Event event = events.stream().findFirst().get();
        assertEquals(EventType.of("IMAGE"), event.getType());
        assertEquals(EventVerb.of("UPLOAD"), event.getVerb());
        assertEquals(EventKey.of("d8ede43b1d9f"), event.getKey());
        assertEquals(newUTCDate(2017, 0, 6, 12, 47, 12, 344), new Date(event.getTimestamp()));
        assertEquals("96f55c7d8f42", event.getAttribute("customer_id").getValue());
        assertEquals("Canon", event.getAttribute("camera_make").getValue());
        assertEquals("EOS 80D", event.getAttribute("camera_model").getValue());
        assertTrue(event.getTags().isEmpty());
    }

    //
    @Test
    public void testImageUpload_NoCustomerId() throws IOException {
        IngestService service = new IngestService(
            withImageUploadProcessor(new IngestProcessorRegistry())
        );

        String json = "{\"type\": \"IMAGE\", \"verb\": \"UPLOAD\", \"key\": \"d8ede43b1d9f\", \"event_time\": \"2017-01-06T12:47:12.344Z\", \"camera_make\": \"Canon\", \"camera_model\": \"EOS 80D\"}";

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), json);
        assertEquals(0, events.size());
    }

    // {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06T12:55:55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "USD 12.34"}
    @Test
    public void testSimpleOrder() throws IOException {
        IngestService service = new IngestService(
            withOrderProcessor(new IngestProcessorRegistry())
        );

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), jsonOrder);
        assertEquals(1, events.size());

        Event event = events.stream().findFirst().get();
        assertEquals(EventType.of("ORDER"), event.getType());
        assertEquals(EventVerb.of("NEW"), event.getVerb());
        assertEquals(EventKey.of("68d84e5d1a43"), event.getKey());
        assertEquals(newUTCDate(2017, 0, 6, 12, 55, 55, 555), new Date(event.getTimestamp()));
        assertEquals("96f55c7d8f42", event.getAttribute("customer_id").getValue());
        assertEquals(Money.parse("USD 12.34"), event.getAttribute("total_amount").getValue());
        assertTrue(event.getTags().isEmpty());
    }

    @Test
    public void testOrder_NoTotalAmount() throws IOException {
        IngestService service = new IngestService(
            withOrderProcessor(new IngestProcessorRegistry())
        );

        String json = "{\"type\": \"ORDER\", \"verb\": \"NEW\", \"key\": \"68d84e5d1a43\", \"event_time\": \"2017-01-06T12:55:55.555Z\", \"total_amount\": \"USD 12.34\"}";

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), json);
        assertEquals(0, events.size());
    }

    @Test
    public void testOrder_NoCustomerId() throws IOException {
        IngestService service = new IngestService(
            withOrderProcessor(new IngestProcessorRegistry())
        );

        String json = "{\"type\": \"ORDER\", \"verb\": \"NEW\", \"key\": \"68d84e5d1a43\", \"event_time\": \"2017-01-06T12:55:55.555Z\", \"customer_id\": \"96f55c7d8f42\"}";

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), json);
        assertEquals(0, events.size());
    }

    @Test
    public void testArray() throws IOException {
        IngestService service = new IngestService(
            withAllProcessors(new IngestProcessorRegistry())
        );

        Collection<Event> events = service.parseEvents(UUID.randomUUID(), jsonArray);
        assertEquals(4, events.size());


        // verify order
        List<Event> eventList = events.stream().collect(Collectors.toList());

        // SITE-VISIT
        assertEquals(EventType.of("SITE_VISIT"), eventList.get(0).getType());
        // CUSTOMER
        assertEquals(EventType.of("CUSTOMER"), eventList.get(1).getType());
        // IMAGE
        assertEquals(EventType.of("IMAGE"), eventList.get(2).getType());
        // ORDER
        assertEquals(EventType.of("ORDER"), eventList.get(3).getType());
    }

    /////// BEGIN TEST UTILITIES



    private static String jsonCustomer = "{\"type\": \"CUSTOMER\", \"verb\": \"NEW\", \"key\": \"96f55c7d8f42\", \"event_time\": \"2017-01-06T12:46:46.384Z\", \"last_name\": \"Smith\", \"adr_city\": \"Middletown\", \"adr_state\": \"AK\"}";

    private static String jsonSiteVisit = "{\"type\": \"SITE_VISIT\", \"verb\": \"NEW\", \"key\": \"ac05e815502f\", \"event_time\": \"2017-01-06T12:45:52.041Z\", \"customer_id\": \"96f55c7d8f42\", \"tags\": [{\"some key\": \"some value\"}]}";

    private static String jsonImageUpload = "{\"type\": \"IMAGE\", \"verb\": \"UPLOAD\", \"key\": \"d8ede43b1d9f\", \"event_time\": \"2017-01-06T12:47:12.344Z\", \"customer_id\": \"96f55c7d8f42\", \"camera_make\": \"Canon\", \"camera_model\": \"EOS 80D\"}";

    private static String jsonOrder = "{\"type\": \"ORDER\", \"verb\": \"NEW\", \"key\": \"68d84e5d1a43\", \"event_time\": \"2017-01-06T12:55:55.555Z\", \"customer_id\": \"96f55c7d8f42\", \"total_amount\": \"USD 12.34\"}";


    private static String jsonArray =
        "["
        + jsonCustomer + ",\n"
        + jsonSiteVisit+ ",\n"
        + jsonImageUpload+ ",\n"
        + jsonOrder
        + "]";

}
