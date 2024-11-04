package com.liderahenk.custombroker;

import java.io.Serial;
import java.io.Serializable;

public class OnlineStatusMessageDTO implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String subscriptionName;


    public OnlineStatusMessageDTO() {
    }

    // All-args constructor
    public OnlineStatusMessageDTO(String subscriptionName) {
        this.subscriptionName = subscriptionName;

    }

    // Getter and Setter for subscriptionName
    public String getSubscriptionName() {
        return subscriptionName;
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }


    @Override
    public String toString() {
        return "OnlineStatusMessageDTO{" +
                "subscriptionName='" + subscriptionName + '\'' +
                '}';
    }
}
