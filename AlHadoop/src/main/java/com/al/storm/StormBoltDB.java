package com.al.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.al.dao.ContentDao;
import com.al.dao.DimensionDao;
import com.al.db.DBHelper;
import com.al.entity.Content;
import com.al.entity.Dimension;
import com.al.util.DateUtil;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class StormBoltDB extends BaseRichBolt {
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		List<Dimension> countryList = (List<Dimension>)input.getValueByField("country");
		List<Content> contentList = (List<Content>)input.getValueByField("content");
    	
    	Connection conn = DBHelper.getConnection();
		try {
			if(DateUtil.getSecond(0) == 10) { // 0点10秒时删除2天前的数据
				DimensionDao.truncateStormDimensionData(conn);
				ContentDao.truncateStormContentData(conn);
				ContentDao.truncateStormContentDetail(conn);
				System.out.println("truncate table ");
			}
			DimensionDao.saveStormDimensionData(countryList, conn);
			ContentDao.saveStormContentData(contentList, conn);
			ContentDao.saveStormContentDetail(contentList, conn);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBHelper.close(conn);
			countryList.clear();countryList=null;
			contentList.clear();contentList=null;
			this.collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
