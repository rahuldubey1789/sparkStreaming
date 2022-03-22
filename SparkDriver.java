//package working;
//
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.regex.Pattern;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.StreamingContext;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaPairInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import scala.Tuple2;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class SparkDriver {
//	private static final Logger LOG = LoggerFactory.getLogger(SparkDriver.class);
//	public static void main(String[] args) {
//		String topic=args[0];
//		// TODO Auto-generated method stub
//		final Pattern SPACE = Pattern.compile(",");
//		// create a local Spark Context with two working threads
//        SparkConf streamingConf = new SparkConf().setMaster("local[*]").setAppName("PageReader");  //here
//		//SparkConf streamingConf = new SparkConf().setMaster("spark://svc-1-fmserv:7077").setAppName("PageReader").setJars(new String[]{"/ericsson/tor/data/rahul/jars/rahul-0.0.1-SNAPSHOT.jar","/ericsson/tor/data/rahul/jars/rahul-0.0.1-SNAPSHOT-jar-with-dependencies.jar"});
//       // streamingConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
//        LOG.trace("rahul create a Spark Java streaming context");
//        // create a Spark Java streaming context, with stream batch interval
//        JavaStreamingContext jssc = new JavaStreamingContext(streamingConf,Durations.seconds(30));
//     // set checkpoint for demo
//        //jssc.checkpoint(System.getProperty("java.io.tmpdir"));
//        
//        // set up receive data stream from kafka
//      //  Collection<String> topics = Arrays.asList(args[1]);
//        Collection<String> topics = Arrays.asList(topic);  //here
//        Map<String, Object> kafkaParams = new HashMap<String, Object>();
//        kafkaParams.put("bootstrap.servers", "localhost:39092");   //here
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "default");
//        kafkaParams.put("auto.offset.reset", "earliest");
//        kafkaParams.put("enable.auto.commit", false);
//        LOG.trace("rahul kafkaParams ready");
//         
//     // create a Discretized Stream of Java String objects
//        // as a direct stream from kafka (zookeeper is not an intermediate)
//         JavaInputDStream<ConsumerRecord<String, String>> stream =
//        		  KafkaUtils.createDirectStream(
//        				  jssc,
//        		    LocationStrategies.PreferConsistent(),
//        		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//        		  );
//         
//      // Raw Kafka messages (in worker app)
//         final JavaPairDStream<String, String> messages = stream.mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
//             @Override
//             public Tuple2<String, String> call(final ConsumerRecord<String, String> record) {
//              //   System.out.println("---------------------------- START (APP)  ----------------------------------------------");
//                // System.out.println("RAW KAFKA MESSAGE; KEY:= " + record.key() + " : Value = " + record.value());
//                 return new Tuple2<String, String>(record.key(), record.value());
//             }
//         });
//         
//         
//      // Get the lines ;i.e the values of the kafka bus (in worker app)
//         final JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
//             /**
//			 * 
//			 */
//			@Override
//             public String call(final Tuple2<String, String> tuple2) {
//                 System.out.println("LINE IN (value) FROM KAFKA: " + tuple2._2());
//                 String searchKey=tuple2._2().split(",")[0];
//        		 System.out.println("searchKey is "+searchKey+" for id "+Thread.currentThread().getId());
//        		 RemoteHeaderCache cacheInstance=RemoteHeaderCache.getInstance();
//        		 System.out.println("count " +Thread.currentThread().getId()+" is  "+cacheInstance.getCount());
//        		 String value=cacheInstance.get(searchKey);
//        		 System.out.println("value for searchKey "+searchKey+" is "+value);
//                 return tuple2._2();
//             }
//             
//         });
//         
//         
//         
//         LOG.trace("rahul splitting the lines");
//       //Split the lines into words
//         final JavaDStream<String> words = lines.flatMap(new FlatTransformerFunction());
//         
//      /*// Print the words : This will appear in the driver.
//         words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//
//             @Override
//             public void call(final JavaRDD<String> wordsRdd) throws Exception {
//            	 
//                 System.out.println("---------------------------- START (DRIVER) foreachRDD----------------------------------------------");
//                 final List<String> listWords = wordsRdd.collect();
//                 for (final String w : listWords) {
//                     System.out.println("WORDS IN FROM KAFKA: " + w);
//                	 if(w.contains(":")){
//                		 String searchKey=w;
//                		 System.out.println("searchKey is "+searchKey+" for id "+Thread.currentThread().getId());
//                		 String value=getInstance(Thread.currentThread().getId()).get(searchKey);
//                		 System.out.println("value for searchKey "+searchKey+" is "+value);
//                	 }
//                 }
//             }
//
//           private RemoteHeaderCache getInstance(long id) {
//  	        return RemoteHeaderCache.getInstance(id);
//  	    }
//             
//             
//         });*/
//         
//      // Count repeating words
//         final JavaPairDStream<String, Integer> wordpairs = words.mapToPair(new PairFunction<String, String, Integer>() {
//
//             /**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//             public Tuple2<String, Integer> call(final String s) {
//				String searchKey=s;
//				/*if(s.contains(":")){
//                System.out.println("searchKey is "+searchKey+" for id "+Thread.currentThread().getId());
//                String value=getInstance(Thread.currentThread().getId()).get(searchKey);
//                System.out.println("value for searchKey "+searchKey+" is "+value);
//				}*/
//                 return new Tuple2<String, Integer>(s, 1);
//             }
//         });
//
//         final JavaPairDStream<String, Integer> wordCounts = wordpairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//
//             @SuppressWarnings("deprecation")
//             @Override
//             public Integer call(final Integer i1, final Integer i2) {
//                 return i1 + i2;
//             }
//         });
//
//         // Printout In driver
//         wordCounts.print();
//         
//         
//         //the driver creates a receiver on one of the Worker nodes and starts an executor process on the worker
//         jssc.start();
//         try {
//			jssc.awaitTermination();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//}
