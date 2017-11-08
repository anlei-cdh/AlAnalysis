package com.al.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.al.dao.DimensionDao;
import com.al.entity.Content;
import com.al.entity.Dimension;
import com.al.entity.Log;
import com.al.util.StringUtil;

import java.util.*;
import java.util.Map.Entry;

public class StormBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	
	private Map<String,Dimension> mapConfig;
	private Map<String,Dimension> countryMap;
	private Map<String,Content> contentMap;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// 初始化国家维度信息表
		mapConfig = DimensionDao.getDimensionConfigMap();
		// 国家数据集合
		countryMap = new HashMap<String,Dimension>();
		// 稿件数据集合
		contentMap = new HashMap<String,Content>();
	}
	
	int lastsecond = -1; // 最后一条信息的时间戳(秒)
	int sub = 3; // 当前信息的时间和之前存在map里的时间戳间隔阀值
	
	public void execute(Tuple input) {
		byte[] binaryByField = input.getBinaryByField("bytes");
		String value = new String(binaryByField);
		Log log = JSON.parseObject(value, Log.class);
		// 只处理国外的数据
		if(log.getArea() != null && log.getArea().trim().equals("国外")) {
	        int second = getSecond(log); // 得到当前的秒

	        if(lastsecond != -1 && second > lastsecond) {
	        	emitDB(second); // 写入数据库
	        }
	        lastsecond = second; // 存储当前的秒
	        addData(log, second); // 加入数据
		}
		this.collector.ack(input);
	}
	
	/**
	 * 得到当前的秒
	 * @return second
	 */
	private int getSecond(Log log) {
		long ts = log.getTs();
		String millisecond = ts + "000";
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(Long.parseLong(millisecond));
        
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int secondOfHour = calendar.get(Calendar.SECOND);
        int second = hour * 3600 + minute * 60 + secondOfHour;
        
        return second;
	}
	
	/**
	 * 加入数据
	 */
	private void addData(Log log, int second) {
		String uuid = log.getUuid();
		String country = log.getCountry();
		long contentId = Long.parseLong(log.getContentId());
		String url = log.getUrl();
		String title = log.getClearTitle();
		
		url = StringUtil.limitString(url, 500, "utf8");
		title = StringUtil.limitString(title, 500, "utf8");
		// 能映射到国家才加入
        if(mapConfig.containsKey(country)) {
        	int dimeId = mapConfig.get(country).getDimeId();
        	addCountryMap(dimeId, second, uuid); // 加入国家数据
        	if(log.getPagetype() == '1' && title.length() > 5) { // 只加入细览 && 标题长度大于5个字
        		addContentMap(contentId, dimeId, second, uuid, url, title); // 加入稿件数据
        	}
        }
	}
	
	/**
	 * 发射到操作数据库的bolt
	 */
	private void emitDB(int second) {
		List<Dimension> countryList = getCountryList(second);
    	List<Content> contentList = getContentList(second);
    	this.collector.emit(new Values(countryList,contentList));
	}
	
	/**
	 * 获得国家集合
	 * 从加入国家的映射中遍历获取新的集合
	 * 如果比当前时间戳少3秒（阀值）以上的时间戳就加入到新集合，并且删除国家映射。
	 * @return
	 */
	private List<Dimension> getCountryList(int second) {
		List<Dimension> countryList = new ArrayList<Dimension>();
		Iterator<Entry<String, Dimension>> it = countryMap.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Dimension> entry = it.next();
			Dimension value = entry.getValue();
			int oldSecond = value.getSecond();
			if(second - oldSecond >= sub) {
				countryList.add(value);
				it.remove();
			}
		}
		return countryList;
	}
	
	/**
	 * 获得稿件集合
	 * 从加入稿件的映射中遍历获取新的集合
	 * 如果比当前时间戳少3秒（阀值）以上的时间戳就加入到新集合，并且删除稿件映射。
	 * @return
	 */
	private List<Content> getContentList(int second) {
		List<Content> contentList = new ArrayList<Content>();
		Iterator<Entry<String, Content>> it = contentMap.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, Content> entry = it.next();
			Content value = entry.getValue();
			int oldSecond = value.getSecond();
			if(second - oldSecond >= sub) {
				contentList.add(value);
				it.remove();
			}
		}
		return contentList;
	}
	
	/**
	 * 国家映射
	 * key:dimeId + "-" + second
	 */
	private void addCountryMap(int dimeId, int second, String uuid) {
		Dimension dimension = null;
		String key = second + "-" + dimeId;
        if(countryMap.containsKey(key)) {
        	dimension = countryMap.get(key);
        } else {
        	dimension = new Dimension();
	        dimension.setDimeId(dimeId);
	        dimension.setSecond(second);
	        countryMap.put(key, dimension);
        }
        dimension.addReuqest(uuid);
	}
	
	/**
	 * 稿件映射
	 * key:contentId + "-" + dimeId + "-" + second
	 */
	private void addContentMap(long contentId, int dimeId, int second, String uuid, String url, String title) {
		Content content = null;
		String key = second + "-" + contentId + "-" + dimeId;
        if(contentMap.containsKey(key)) {
        	content = contentMap.get(key);
        } else {
        	content = new Content();
        	content.setContentId(contentId);
        	content.setDimeId(dimeId);
	        content.setSecond(second);
	        content.setUrl(url);
	        content.setTitle(title);
	        contentMap.put(key, content);
        }
        content.addReuqest(uuid);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("country","content"));
	}
}
