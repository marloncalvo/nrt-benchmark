package com.marloncalvo.bench.common;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a broker consume event for JSON serialization.
 * Extensible: add new fields as needed for additional event data.
 */
public class BrokerEvent {
    @JsonProperty("brokerConsumeEventTimestamp")
    private long brokerConsumeEventTimestamp;

    // Default constructor for Jackson
    public BrokerEvent() {}

    public BrokerEvent(long brokerConsumeEventTimestamp) {
        this.brokerConsumeEventTimestamp = brokerConsumeEventTimestamp;
    }

    public long getBrokerConsumeEventTimestamp() {
        return brokerConsumeEventTimestamp;
    }

    public void setBrokerConsumeEventTimestamp(long brokerConsumeEventTimestamp) {
        this.brokerConsumeEventTimestamp = brokerConsumeEventTimestamp;
    }
}
