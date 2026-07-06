package com.evn.billing.common.dto;

import lombok.Data;
import java.util.List;

@Data
public class MeterTopology {
    private List<MeterPointNode> rootPoints;
}
