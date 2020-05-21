package com.galaxy;



import backtype.storm.tuple.Values;
import com.galaxy.hbase.common.Utils;
import com.galaxy.kafka.StringMultiSchemeWithTopic;
import com.galaxy.kafka.StringScheme;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;




public class MessageScheme extends StringMultiSchemeWithTopic {

    public Iterable<List<Object>> deserializeWithTopic(String topic, ByteBuffer bytes) {
/*
List<Object> items = new Values(StringScheme.deserializeString(bytes), topic);
List<Object> items = new Values(bytes.array(), topic);
*/
        List<Object> items = new Values(backtype.storm.utils.Utils.toByteArray(bytes), topic);
        return Collections.singletonList(items);
    }
}
