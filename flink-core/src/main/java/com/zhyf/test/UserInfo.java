package com.zhyf.test;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserInfo {
    private int id;
    private String gender;
    private String city;
}
