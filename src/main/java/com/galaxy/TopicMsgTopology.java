package com.galaxy;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.galaxy.hbase.bolt.mapper.HBaseMapper;
import org.apache.log4j.PropertyConfigurator;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.*;

import com.galaxy.MessageScheme;
import com.galaxy.TopicMsgBolt;
import com.galaxy.MyHBaseMapper;
import com.galaxy.hbase.bolt.HBaseBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.galaxy.kafka.*;

public class TopicMsgTopology {

    private static final Logger logger = LoggerFactory.getLogger(TopicMsgTopology.class);

    public static final String STRING_KAFKA_BOLT = "kafka_bolt";
    public static final String STRING_KAFKA_BOLT2= "kafka_bolt_index";
    public static final String STRING_HBASE_BOLT = "hbase_bolt";
    public static final String STRING_HBASE_BOLT2= "hbase_bolt_index";

    private static Properties props_;

    private static void config(){
        String connectdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "log4j.properties";
        PropertyConfigurator.configure(connectdir);
    }


    private static boolean readConfig(String filenames)  {
        try{
            props_ = new Properties();
            FileInputStream fis = new FileInputStream(filenames);
            if ( fis == null ){
                return false;
            }
            props_.load(fis);
            fis.close();
            return true;
        } catch (IOException e){
            e.printStackTrace();
            return false;
        }
    }
    private static List<SpoutConfig> getSpoutConfig(){
        // get topcis
        String topicList = props_.getProperty("kafka.topics.list");
        if ( topicList == null || topicList.length()==0 ){
            logger.error("Get config kafka.topics.list failed!");
            return null;
        }
        // get brokerlist
        String brokerList = props_.getProperty("broker.list");
        if ( brokerList == null ){
            logger.error("Get config broker.list failed!");
            return null;
        }
        List<SpoutConfig> spoutConfigs = new ArrayList();
        if ( spoutConfigs == null){
            return null;
        }
        BrokerHosts brokerHosts = new ZkHosts(brokerList);
        String forceStartOff = props_.getProperty("kafka.forceStartOff", "false");
        for ( String topic : topicList.split(",")){
            // set spout config
            SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, "/" + topic, UUID.randomUUID().toString());
            spoutConfig.scheme = new MessageScheme();
            if ( forceStartOff.equalsIgnoreCase("false") == true ){
                spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
            }
            spoutConfigs.add(spoutConfig);
        }
        return spoutConfigs;
    }

    private static HBaseBolt getHBaseBolt(HBaseMapper mapper, String tableName){
        try{
            if ( tableName.length() == 0 ){
                logger.error("Get hbase.table item failed.");
                return null;
            }
            // get hbase info
            String batchSize = props_.getProperty("hbase.batchsize", "1000");
            String flushInterval = props_.getProperty("hbase.flushIntervalSecs", "2");
            int size = Integer.parseInt(batchSize);
            int flush = Integer.parseInt(flushInterval);
            HBaseBolt hBaseBolt = new HBaseBolt(tableName, mapper).withConfigKey("hbase.conf").withBatchSize(size).withFlushIntervalSecs(flush);
            return hBaseBolt;
        } catch (NumberFormatException e){
            e.printStackTrace();
            return null;
        }

    }

//    public static void main(String[] args){
//        long ts = 1523602633;
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String date = sdf.format(new Date(ts*1000));
//        System.out.println(date);
//    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        config();

        // 1 读取配置文件
        File directory = new File("conf");
        String curPath = directory.getAbsolutePath();
        String confFile = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "config.properties";
        if ( false == readConfig(confFile) ){
            logger.error("Read config failed!");
            return;
        }

        // 2 get spoutconfig
        List<SpoutConfig> spoutConfigs = getSpoutConfig();
        if ( null == spoutConfigs){
            logger.error("Get spoutconfigs failed!");
            return ;
        }

        // 3 拓扑
        TopologyBuilder builder = new TopologyBuilder();

        String kafkaSpoutThreadConfig = props_.getProperty("task.kafkaSpout.num", "8");
        String kafkaBoltThreadsConfig = props_.getProperty("task.kafkaBolt.num", "8");
        String kafkaBoltIndexThreadConfig = props_.getProperty("task.kafkaBolt.index.num", "8");
        String hbaseBoltThreadsConfig = props_.getProperty("task.hbaseBolt.num", "8");
        String hbaseBoltIndexThreadConfig = props_.getProperty("task.hbaseBolt.index.num", "8");
        int nKafkaSpoutThreads= Integer.parseInt(kafkaSpoutThreadConfig);
        int nKafkaBoltThreads = Integer.parseInt(kafkaBoltThreadsConfig);
        int nKafkaBoltIndexThreads = Integer.parseInt(kafkaBoltIndexThreadConfig);
        int nHbaseBoltThreads = Integer.parseInt(hbaseBoltThreadsConfig);
        int hHbaseBoltIndexThreads = Integer.parseInt(hbaseBoltIndexThreadConfig);

        // 3.1 kafka bolts
        BoltDeclarer boltDeclarer = builder.setBolt(STRING_KAFKA_BOLT, new TopicMsgBolt(props_),nKafkaBoltThreads);
        BoltDeclarer boltDeclarer2 = builder.setBolt(STRING_KAFKA_BOLT2, new TopicMsgBolt2(props_), nKafkaBoltIndexThreads);
        for(SpoutConfig spoutConfig : spoutConfigs){
            builder.setSpout(spoutConfig.topic, new KafkaSpout(spoutConfig), nKafkaSpoutThreads);
            boltDeclarer.shuffleGrouping(spoutConfig.topic);
            boltDeclarer2.shuffleGrouping(spoutConfig.topic);
        }

        // 3.2 hbase bolt
        String strTable = props_.getProperty("hbase.table");
        HBaseMapper mapper = new MyHBaseMapper(props_);
        HBaseBolt hBaseBolt = getHBaseBolt(mapper, strTable);
        if ( hBaseBolt == null ){
            return;
        }
        builder.setBolt(STRING_HBASE_BOLT, hBaseBolt, nHbaseBoltThreads).shuffleGrouping(STRING_KAFKA_BOLT);

        // 3.2 hbase bolt index
        strTable = props_.getProperty("hbase.index.table");
        mapper = new IndexHBaseMapper(props_);
        HBaseBolt hBaseBolt2 = getHBaseBolt(mapper, strTable);
        builder.setBolt(STRING_HBASE_BOLT2, hBaseBolt2, hHbaseBoltIndexThreads).shuffleGrouping(STRING_KAFKA_BOLT2);

        // 4
        String strWorkers = props_.getProperty("worker.number", "4");
        int workes = Integer.parseInt(strWorkers);
        String strAckers = props_.getProperty("acker.number", "1");
        int ackers = Integer.parseInt(strAckers);
        // 5 配置
        Config conf = new Config();
        Map<String, Object> hbConf = new HashMap();
        String rootDir = props_.getProperty("hbase.rootdir");
        String zkQuorum = props_.getProperty("hbase.zookeeper.quorum");
        String zkPort = props_.getProperty("hbase.zookeeper.property.clientPort");
        String zkDataDir = props_.getProperty("hbase.zookeeper.property.dataDir");

        if ( rootDir.length() == 0 || zkQuorum.length() == 0 ){
            logger.error("Get hbase.rootdir or hbase.zookeeper.quorum config failed.");
            return ;
        }
        hbConf.put("hbase.rootdir", rootDir);
        hbConf.put("hbase.zookeeper.quorum", zkQuorum);
        hbConf.put("hbase.zookeeper.property.clientPort", zkPort);
        //hbConf.put("hbase.zookeeper.property.dataDir", zkDataDir);
        conf.put("hbase.conf", hbConf);
        String bSingleThread = props_.getProperty("spout.single.thread", "true");
        conf.put("spout.single.thread", bSingleThread);
        conf.setNumAckers(ackers);

        // 6 提交拓扑
        if ( args.length == 0 ){
            String name = "kafkaTopicTopology";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Utils.sleep(500000);
            cluster.killTopology("kafkaTopicTopology");
            cluster.shutdown();
        } else {
            conf.setNumWorkers(workes);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }catch (AlreadyAliveException e){
                e.printStackTrace();
            }catch (InvalidTopologyException e){
                e.printStackTrace();
            }
        }
    }

}
