package org.xq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author xuqi
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    public String user;
    public String url;
    public Long timestamp;
}
