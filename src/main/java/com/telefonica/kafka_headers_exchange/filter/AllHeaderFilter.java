package com.telefonica.kafka_headers_exchange.filter;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.header.Headers;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "spring.kafka.header.filter.match", havingValue = "all")
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
