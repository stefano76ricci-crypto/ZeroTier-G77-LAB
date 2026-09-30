package net.kaaass.zerotierfix.events;

import lombok.Data;

@Data
public class RefreshUdpResultEvent {
    private final boolean success;
    private final String message;
}
