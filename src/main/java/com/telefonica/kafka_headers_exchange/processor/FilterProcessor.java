package com.telefonica.kafka_headers_exchange.processor;

import java.util.List;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.telefonica.weblogic_kafka_integration.model.Event;

public class FilterProcessor implements Processor<String, Event, String, Event> {

    private static final Logger logger = LoggerFactory.getLogger(FilterProcessor.class);

    private ProcessorContext<String, Event> context;

    private List<String> filterKeys;
    
    private List<String> filterValues;

    private FilterStr filter;

    private boolean applyNot;

    private String queue;

    public FilterProcessor(String queue, List<String> filterKeys, List<String> filterValues, FilterStr filter) {
        this(queue, filterKeys, filterValues, filter, false);
    }
    
    public FilterProcessor(String queue, List<String> filterKeys, List<String> filterValues, 
        FilterStr filter, boolean applyNot) {
        this.filterKeys = filterKeys;
        this.filterValues = filterValues;
        this.filter = filter;
        this.applyNot = applyNot;
        this.queue = queue;
    }

    @Override
    public void init(ProcessorContext<String, Event>  context) {
        this.context = context;
    }
    
    @Override
    public void process(Record<String, Event> record) {
        boolean match = filter.applyFilter(filterKeys, filterValues, record.headers());

        if(applyNot?!match:match) {
            logger.info((applyNot?"NON-MATCHING FILTER,":"MATCHING FILTER,") + "Forwarding record with key: {}, value: {} to Queue: {}.",
                record.key(), record.value(), queue);
            context.forward(record);
        }

    }

}