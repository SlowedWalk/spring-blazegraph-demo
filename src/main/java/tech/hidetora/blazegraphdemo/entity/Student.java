package tech.hidetora.blazegraphdemo.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@Builder
public class Student {
    private String name;
    private String email;
    private String phone;
    private String address;


}
