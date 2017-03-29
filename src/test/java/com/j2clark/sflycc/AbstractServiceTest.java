package com.j2clark.sflycc;

import com.j2clark.sflycc.processors.CustomerIngestProcessor;
import com.j2clark.sflycc.processors.ImageUploadIngestProcessor;
import com.j2clark.sflycc.processors.IngestProcessorRegistry;
import com.j2clark.sflycc.processors.OrderIngestProcessor;
import com.j2clark.sflycc.processors.SiteVisitIngestProcessor;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public abstract class AbstractServiceTest {

    public Date newUTCDate(int year, int month, int dayOfMonth, int hourOfDay, int minute, int second, int ms) {
        Calendar c = GregorianCalendar.getInstance();
        c.setTimeZone(TimeZone.getTimeZone("UTC"));
        c.set(year, month, dayOfMonth, hourOfDay, minute, second);
        c.set(Calendar.MILLISECOND, ms);
        return c.getTime();
    }

    protected IngestProcessorRegistry withCustomerProcessor(IngestProcessorRegistry registry) {
        new CustomerIngestProcessor(registry);
        return registry;
    }

    protected IngestProcessorRegistry withSiteVisitProcessor(IngestProcessorRegistry registry) {
        new SiteVisitIngestProcessor(registry);
        return registry;
    }

    protected IngestProcessorRegistry withImageUploadProcessor(IngestProcessorRegistry registry) {
        new ImageUploadIngestProcessor(registry);
        return registry;
    }

    protected IngestProcessorRegistry withOrderProcessor(IngestProcessorRegistry registry) {
        new OrderIngestProcessor(registry);
        return registry;
    }

    protected IngestProcessorRegistry withAllProcessors(IngestProcessorRegistry registry) {
        new CustomerIngestProcessor(registry);
        new SiteVisitIngestProcessor(registry);
        new ImageUploadIngestProcessor(registry);
        new OrderIngestProcessor(registry);
        return registry;
    }

    public String readJson(String fileName) {
        try {
            return new String(Files.readAllBytes(
                Paths.get(ClassLoader.getSystemResource("json/" + fileName).toURI())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
