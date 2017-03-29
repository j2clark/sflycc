package com.j2clark.sflycc.reports;

import org.joda.money.Money;

/**
 * DTO for holding report generated data
 * Not part of the domain
 */
public class Customer {

    private final String customerId;
    private final Money ltv;

    public Customer(String customerId, Money ltv) {
        this.customerId = customerId;
        this.ltv = ltv;
    }

    public String getCustomerId() {
        return customerId;
    }

    public Money getLtv() {
        return ltv;
    }

    public String toString() {
        return "Customer{id["+customerId+"], ltv["+ltv+"]}";
    }
}
