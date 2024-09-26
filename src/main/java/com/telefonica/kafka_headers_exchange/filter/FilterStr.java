package com.telefonica.kafka_headers_exchange.filter;

import java.util.List;

import org.apache.kafka.common.header.Headers;

public interface FilterStr {

    public boolean applyFilter(List<String> filterKeys, List<String> filterValues, Headers headers);

}
