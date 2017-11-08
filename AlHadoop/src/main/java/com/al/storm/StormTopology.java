package com.al.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

public class StormTopology {

	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts(com.al.config.Config.zkHosts);
		String topic = com.al.config.Config.topic;
		String zkRoot = "/aura";
		String id = UUID.randomUUID().toString();
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
		
		String SPOUT_ID = KafkaSpout.class.getSimpleName();
		String BOLT_TV_ID = StormBolt.class.getSimpleName();
		String BOLT_DB_ID = StormBoltDB.class.getSimpleName();

		/**
		 * 本地模式提交
		 */
		if(com.al.config.Config.is_local) {
			topologyBuilder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf));
			topologyBuilder.setBolt(BOLT_TV_ID, new StormBolt()).shuffleGrouping(SPOUT_ID);
			topologyBuilder.setBolt(BOLT_DB_ID, new StormBoltDB()).shuffleGrouping(BOLT_TV_ID);

			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(StormTopology.class.getSimpleName(), new Config(), topologyBuilder.createTopology());
		} else {
			/**
			 * 集群模式提交
			 */
			topologyBuilder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf));
			topologyBuilder.setBolt(BOLT_TV_ID, new StormBolt(), 5).setNumTasks(10).shuffleGrouping(SPOUT_ID); // executor:5 task:10
			topologyBuilder.setBolt(BOLT_DB_ID, new StormBoltDB()).shuffleGrouping(BOLT_TV_ID);

			try {
				Config config = new Config();
				config.setNumWorkers(2); // workder:2
				StormSubmitter.submitTopology(StormTopology.class.getSimpleName(), config, topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
	}

}
