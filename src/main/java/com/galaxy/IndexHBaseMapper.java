package com.galaxy;

import backtype.storm.tuple.Tuple;
import com.galaxy.hbase.bolt.mapper.HBaseMapper;
import com.galaxy.hbase.common.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class IndexHBaseMapper implements HBaseMapper {

    private static final Logger logger = LoggerFactory.getLogger(IndexHBaseMapper.class);
    private final Properties props_;
    private static final String INDEX_PREFIX = "hbase.index.pvid.";
    private ColumnList colList;

    public IndexHBaseMapper(Properties props){
        this.props_ = props;
    }

    public byte[] rowKey(Tuple tuple) {
        return tuple.getStringByField("Id").getBytes();
    }

    public ColumnList columns(Tuple tuple) throws UnsupportedEncodingException {
        // 读取配置文件
        String strKey = INDEX_PREFIX + tuple.getStringByField("topic");
        String strColumnInfo = this.props_.getProperty(strKey);
        if ( strColumnInfo == null || strColumnInfo.length() == 0 ){
            logger.error("Get item " + strKey + " failed");
            return null;
        }
        String[] sItems = strColumnInfo.split(":");
        if ( sItems.length < 2 ){
            return null;
        }
        // 值数组
        List<String> values = (ArrayList<String>)tuple.getValueByField("Message");
        // 名称数组
        String[] names  = sItems[1].split(",");
        if ( values.size() > names.length ){
            return null;
        }
        //
        ColumnList cols = new ColumnList();
        for ( int i=0; i < names.length; ++i ){
            cols.addColumn(sItems[0].getBytes(), names[i].getBytes(), values.get(i).getBytes("UTF8"));
        }
        return cols;
    }
}
