package com.j2clark.sflycc;

import com.j2clark.sflycc.domain.Data;
import com.j2clark.sflycc.processors.IngestProcessorRegistry;
import com.j2clark.sflycc.reports.Customer;
import com.j2clark.sflycc.reports.CustomerReportService;
import com.j2clark.sflycc.services.IngestService;

import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class CustomerReportServiceTest extends AbstractServiceTest {

    @Test
    public void testTopX() throws Exception {
        IngestService ingestService = new IngestService(withAllProcessors(new IngestProcessorRegistry()));
        CustomerReportService reportService = new CustomerReportService();

        String json = readJson("topX.json");

        Data data = ingestService.ingest(UUID.randomUUID(), json);

        int count = 3;
        List<Customer>
            customerList = reportService.topXSimpleLTVCustomers(count, data);

        for (Customer c : customerList) {
            System.out.println(c);
        }
    }
}
