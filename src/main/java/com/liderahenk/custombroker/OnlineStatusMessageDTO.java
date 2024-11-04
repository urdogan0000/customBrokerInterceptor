package com.liderahenk.custombroker;

import java.io.Serial;
import java.io.Serializable;

public class OnlineStatusMessageDTO implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String subscriptionName;
    private String eventDate;

    // No-args constructor required for deserialization
    public OnlineStatusMessageDTO() {
    }

    // All-args constructor
    public OnlineStatusMessageDTO(String subscriptionName, String eventDate) {
        this.subscriptionName = subscriptionName;
        this.eventDate = eventDate;
    }

    // Getter and Setter for subscriptionName
    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    // Getter and Setter for eventDate
    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    @Override
    public String toString() {
        return "OnlineStatusMessageDTO{" +
                "subscriptionName='" + subscriptionName + '\'' +
                ", eventDate='" + eventDate + '\'' +
                '}';
    }
}
