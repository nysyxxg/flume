package org.apache.flume.source.my;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractEventDrivenSource;

import java.util.HashMap;
import java.util.Map;

public class MySelfSource  extends AbstractEventDrivenSource {
    @Override
    protected void doConfigure(Context context) throws FlumeException {

    }

    @Override
    protected void doStart() throws FlumeException {
        ChannelProcessor channelProcessor = this.getChannelProcessor();
        Map<String,String>  map = new HashMap<>();
        map.put("ower","XUPC");
        map.put("date","2016/09/29");
        Event  e = null;
        for (int i = 0; i <10000 ; i++) {
            e = new SimpleEvent();
            e.setHeaders(map);
            e.setBody(("tom--"+i).getBytes());
            channelProcessor.processEvent(e);

        }

    }

    @Override
    protected void doStop() throws FlumeException {

    }
}
