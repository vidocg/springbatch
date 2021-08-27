package com.model;

import lombok.Data;

@Data
public class Account {
    private String id;
    private String userName;
    private String userEmail;
    private Boolean completed;
    private String userBrand;
}
