package com.galaxy;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import scala.Int;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class TopicMsgBolt2 extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(TopicMsgBolt.class);
    private final Properties props_;
    private static final long UINT32_MAX_VALUE = 0x00000000FFFFFFFFL;

    public TopicMsgBolt2(Properties props){
        this.props_ = props;
    }

    private String uint64ToString(long value){
        long h = value >> 32 & 0xFFFFFFFFL;
        long l = value & 0x00000000FFFFFFFFL;
        BigInteger bH = BigInteger.valueOf(h);
        BigInteger bL  = BigInteger.valueOf(l);
        BigInteger m = bH.shiftLeft(32);
        BigInteger n = m.or(bL);
        return n.toString();
    }
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try{
            String topic = (String) tuple.getValue(1);
            String strUid = "";
            List<String> values = new ArrayList<String>();
            String pbStr = this.props_.getProperty(topic);

            // PB 数据解析
            if ( pbStr.equals("ImeInputDataPb") ){
                UISInterface.ImeInputDataPb imPB = UISInterface.ImeInputDataPb.parseFrom(tuple.getBinary(0));
                String uid = uint64ToString(imPB.getUid());
                long  ts = imPB.getTimestamp();
                String inputStr = imPB.getInputStr().toString("GBK");
                String pvid     = uint64ToString(imPB.getPvid());
                String entryStr = imPB.getEntityStr().toString("GBK");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long randomId = imPB.getRandomId();
                long randomSeq = imPB.getRandomSequence();
                long millsec = ts;
                String date = sdf.format(new Date(millsec*1000));
                // 数值
                values.add(date);
                values.add(inputStr);
                values.add(entryStr);
                values.add(Long.toString(randomId));
                values.add(Long.toString(randomSeq));
                strUid = pvid + "_" + uid;

            } else if ( pbStr.equals("PvRecordDataPb") ){
                UISInterface.PvRecordDataPb pvPB = UISInterface.PvRecordDataPb.parseFrom(tuple.getBinary(0));
                String uid = uint64ToString(pvPB.getUid());
                int ts = pvPB.getTimestamp();
                String pvid     = uint64ToString(pvPB.getPvid());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long millsec = ts;
                String date = sdf.format(new Date(millsec*1000));
                //数值
                values.add(date);
                values.add("Y");
                strUid =  pvid + "_" + uid;
            } else if ( pbStr.equals("ClickRecordDataPb") ) {
                UISInterface.ClickRecordDataPb clickPB = UISInterface.ClickRecordDataPb.parseFrom(tuple.getBinary(0));
                String uid = uint64ToString(clickPB.getUid());
                int ts = clickPB.getTimestamp();
                String pvid     = uint64ToString(clickPB.getPvid());
                int adType = clickPB.getAdType();
                String keywordStr = clickPB.getKeywordStr().toString("GBK");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long millsec = ts;
                String date = sdf.format(new Date(millsec*1000));

                //数值
                values.add(date);
                values.add(Integer.toString(adType));
                values.add(keywordStr);
                values.add("Y");
                strUid = pvid + "_" + uid;
            }
             else {

            }

            // 数据提交
            basicOutputCollector.emit(new Values(strUid, topic, values));

        } catch (InvalidProtocolBufferException e){
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Id","topic", "Message"));
    }
}
