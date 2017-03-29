package com.j2clark.sflycc.reports;

import com.j2clark.sflycc.domain.Data;
import com.j2clark.sflycc.domain.EventType;
import com.j2clark.sflycc.domain.Event;

import org.joda.money.Money;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public class CustomerReportService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /*
        A simple LTV can be calculated using the following equation:
            52(a) x t.
            Where a is the average customer value per week (customer expenditures per visit (USD) x number of site visits per week)
            and t is the average customer lifespan. The average lifespan for Shutterfly is 10 years.
         */
    public List<Customer> topXSimpleLTVCustomers(int resultCount, Data d) {

        List<Customer> customers = new ArrayList<>();
        if (!d.getEvents().isEmpty()) {

            // sort according to timestamp
            Collection<Event> sorted = d.getEvents().stream()
                .sorted((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp()))
                .collect(Collectors.toList());


            // first group all important events by customer - since each will have their own time range
            Map<String,Map<String,List<Event>>> customerEvents = new HashMap<>();
            for (Event event : sorted) {
                if (EventType.of("ORDER").equals(event.getType()) ||
                    EventType.of("SITE_VISIT").equals(event.getType())) {

                    String customerID = (String) event.getAttribute("customer_id").getValue();
                    Map<String, List<Event>> customer = customerEvents.get(customerID);
                    if (customer == null) {
                        customer = new HashMap<>();
                        customer.put("ORDER", new ArrayList<>());
                        customer.put("SITE_VISIT", new ArrayList<>());
                        customer.put("ALL", new ArrayList<>());
                        customerEvents.put(customerID, customer);
                    }

                    customer.get("ALL").add(event);
                    if (EventType.of("ORDER").equals(event.getType())) {
                        customer.get("ORDER").add(event);
                    } else {
                        customer.get("SITE_VISIT").add(event);
                    }
                }
            }

            // now for each customer, calculate LTV
            // this can totally be forked into separate jobs for a performance boost
            for (String customerId: customerEvents.keySet()) {

                Map<String,List<Event>> customer = customerEvents.get(customerId);
                List<Event> all = customer.get("ALL");

                // find the min/max timestamps
                OptionalLong minTimestamp = all.stream().mapToLong(Event::getTimestamp).min();
                OptionalLong maxTimestamp = all.stream().mapToLong(Event::getTimestamp).max();
                if (maxTimestamp.isPresent() && minTimestamp.isPresent()) {

                    // what is our date range, and is it enough to get meaningful data?
                    // avoiding that check for now
                    long range = maxTimestamp.getAsLong() - minTimestamp.getAsLong();

                    int visits = customer.get("SITE_VISIT").size();
                    Optional<Money> totalSpent = Optional.empty();
                    for (Event event : customer.get("ORDER")) {
                        Money orderAmount = (Money) event.getAttribute("total_amount").getValue();
                        if (totalSpent.isPresent()) {
                            totalSpent = Optional.of(totalSpent.get().plus(orderAmount));
                        } else {
                            totalSpent = Optional.of(orderAmount);
                        }
                    }

                    if (totalSpent.isPresent()) {
                        // we have data to work with... calculate

                        // very simple approach to determine week
                        // based simple on duration spanned by events
                        long WEEK = 1000 * 60 * 60 * 24 * 7;
                        int weeks = (int) range/ (int) WEEK; // do we care about rounding?
                        if (weeks < 1) weeks = 1;

                        // total/visits = expenditure per visit
                        Money avgPerVisit = totalSpent.get().dividedBy(visits, RoundingMode.HALF_UP);

                        // what is overall time period? in weeks?
                        // visits/week = visits/weeks
                        double visitsPerWeek = visits/weeks;

                        /*
                        52(a) x t.
                        Where a is the average customer value per week (customer expenditures per visit (USD) x number of site visits per week)
                        and t is the average customer lifespan. The average lifespan for Shutterfly is 10 years.
                         */

                        // not sure I got the math right here - seems fishy when it resolves to (visit/visit)*something
                        Money ltv = avgPerVisit.multipliedBy(visitsPerWeek, RoundingMode.HALF_UP).multipliedBy(52).multipliedBy(10);
                        System.out.println("range["+range+" ms], weeks["+weeks+"], visits["+visits+"], spent["+totalSpent.get()+"], LTV["+ltv.toString()+"]");

                        customers.add(new Customer(customerId, ltv));
                    } else {
                        // nothing going on with this "customer" - drop from list
                    }
                }
            }
            // sort and limit results

        }
        return customers.stream().sorted(
            (c1, c2) -> c2.getLtv().getAmount().compareTo(c1.getLtv().getAmount())
        ).limit(resultCount).collect(Collectors.toList());
    }

}
