/*
 * File: NetworkEvent.java
 * Author: Jindrich Vodak (xvodak06)
 * Date: 31. 12. 2025
 */

package pipeline;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NetworkEvent {
    public long eventTime;
    public String srcIp;
    public String dstIp;
    public int srcPort;
    public int dstPort;
    public boolean syn;
}