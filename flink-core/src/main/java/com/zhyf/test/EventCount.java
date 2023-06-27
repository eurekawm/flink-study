package com.zhyf.test;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;
}
