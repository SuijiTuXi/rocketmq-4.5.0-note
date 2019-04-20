package org.apache.rocketmq.remoting.protocol;

import java.util.HashMap;
import java.util.Map;

public class RequestCodeToName {
    private static Map<Integer, String> nameMapping = new HashMap<Integer, String>();

    static {
        nameMapping.put(11, "PULL_MESSAGE");
        nameMapping.put(14, "QUERY_CONSUMER_OFFSET");
        nameMapping.put(15, "UPDATE_CONSUMER_OFFSET");
        nameMapping.put(34, "HEART_BEAT");
        nameMapping.put(35, "UNREGISTER_CLIENT");
        nameMapping.put(38, "GET_CONSUMER_LIST_BY_GROUP");

        nameMapping.put(103, "REGISTER_BROKER");
        nameMapping.put(105, "GET_ROUTEINTO_BY_TOPIC");
        nameMapping.put(310, "SEND_MESSAGE_V2");
    }

    public static String getCommandName(int requestCode) {
        if (nameMapping.get(requestCode) != null) {
            return nameMapping.get(requestCode);
        } else {
            return String.valueOf(requestCode);
        }
    }
}
