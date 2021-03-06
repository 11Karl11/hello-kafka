package com.karl.one;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author karl xie
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Company {

    private String name;
    private String address;

}