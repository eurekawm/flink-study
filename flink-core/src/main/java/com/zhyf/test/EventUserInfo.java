package com.zhyf.test;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventUserInfo {
    private int id;
    private String eventId;
    private int cnt;
    private String gender;
    private String city;
}
