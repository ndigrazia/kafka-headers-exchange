package com.telefonica.kafka_headers_exchange.processor;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.header.Headers;

public class AllHeaderFilter implements FilterStr {

    @Override
    public boolean applyFilter(List<String> filterKeys, List<String> filterValues, Headers headers) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(headers.iterator(), Spliterator.ORDERED), 
        false).filter(
            h-> {
                int index = filterKeys.indexOf(h.key());
                
                if(index !=-1 && filterValues.get(index).equalsIgnoreCase(new String(h.value())))
                    return true;
    
                return false;
            }
        ).count() == filterKeys.size();
    }

}
