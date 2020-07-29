package it.istat.is2.catalogue.relais.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import it.istat.is2.catalogue.relais.utility.RelaisUtility;
import it.istat.is2.workflow.engine.EngineService;
import scala.Tuple2;

@Component
public class SparkService {
	
	final static int stepService = 250;
	final static int sizeFlushed = 20;
	final static String INDEX_SEPARATOR = ";";
	final static String PREFIX_PATTERN = "P_";

	final public static String codeMatchingA = "X1";
	final public static String codeMatchingB = "X2";
	final static String codContengencyTable = "CT";
	final static String codContengencyIndexTable = "CIT";
	final static String codMachingTable = "MT";
	final static String codPossibleMachingTable = "PM";
	final static String codResidualA = "RA";
	final static String codResidualB = "RB";
	final static String codeFS = "FS";
	final static String codeP_POST = "P_POST";
	final static String codeRATIO = "R";
	final static String params_MatchingVariables = "MATCHING_VARIABLES";
	final static String params_ThresholdMatching = "THRESHOLD_MATCHING";
	final static String params_ThresholdUnMatching = "THRESHOLD_UNMATCHING";
	final static String params_BlockingVariables = "BLOCKING VARIABLES";
	final static String params_BlockingVariablesA = "BLOCKING A";
	final static String params_BlockingVariablesB = "BLOCKING B";
	final static String codeBlockingVariablesA = "BA";
	final static String codeBlockingVariablesB = "BB";
	final static String params_ReductionMethod = "REDUCTION_METHOD";
	final static String VARIABLE_FREQUENCY = "FREQUENCY";
	final static String ROW_IA = "ROW_A";
	final static String ROW_IB = "ROW_B";
	

	@Autowired
	private ContingencyService contingencyService;
	
	public Map<?, ?> pRLContingencyTableCartesianProductSpark(Long idelaborazione,
			Map<String, List<String>> ruoliVariabileNome, Map<String, Map<String, List<String>>> worksetIn,
			Map<String, String> parametriMap) throws Exception {

		 System.setProperty("hadoop.home.dir", "C:\\winutils");
		
		Instant start = Instant.now();

		final Map<String, Map<?, ?>> returnOut = new HashMap<>();
		final Map<String, Map<?, ?>> worksetOut = new HashMap<>();

		final Map<String, ArrayList<String>> contengencyTableOut = new LinkedHashMap<>();
		final Map<String, ArrayList<String>> rolesOut = new LinkedHashMap<>();
		final Map<String, String> rolesGroupOut = new HashMap<>();

		ArrayList<String> variabileNomeListMA = new ArrayList<>();
		ArrayList<String> variabileNomeListMB = new ArrayList<>();

		ArrayList<String> variabileNomeListOut = new ArrayList<>();

		////logService.save("Process Contingency Table Starting...");

		ruoliVariabileNome.get(codeMatchingA).forEach((varname) -> {
			variabileNomeListMA.add(varname);
		});
		////logService.save("Matching variables dataset A: " + variabileNomeListMA);
		ruoliVariabileNome.get(codeMatchingB).forEach((varname) -> {
			variabileNomeListMB.add(varname);
		});
		////logService.save("Matching variables dataset B: " + variabileNomeListMB);
		ruoliVariabileNome.values().forEach((list) -> {
			variabileNomeListOut.addAll(list);
		});

		String firstFiledMA = ruoliVariabileNome.get(codeMatchingA).get(0);
		String firstFiledMB = ruoliVariabileNome.get(codeMatchingB).get(0);
		int sizeA = worksetIn.get(codeMatchingA).get(firstFiledMA).size();
		int sizeB = worksetIn.get(codeMatchingB).get(firstFiledMB).size();

		try {
			contingencyService.init(parametriMap.get(params_MatchingVariables));
		} catch (Exception e) {
			////logService.save("Error parsing " + params_MatchingVariables);
			throw new Exception("Error parsing " + params_MatchingVariables);
		}
		List<String> nameMatchingVariables = new ArrayList<>();

		contingencyService.getMetricMatchingVariableVector().forEach(metricsm -> {
			contengencyTableOut.put(metricsm.getMatchingVariable(), new ArrayList<>());
			nameMatchingVariables.add(metricsm.getMatchingVariable());
		});

	 
		//final Map<String, List<String>> coupledIndexByPattern = RelaisUtility.getEmptyMapByKey(
		//		contengencyTable.keySet().stream().filter(key -> Integer.parseInt(key) > 0), PREFIX_PATTERN);

		// Create a local StreamingContext with two working thread and batch interval of
		// 1 second
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark MultipleContest Test").set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.driver.allowMultipleContexts", "true");
		conf.setMaster("local[*]");

		// JavaStreamingContext jssc = new JavaStreamingContext(conf,
		// Durations.seconds(1));


SparkContext sparkc = new SparkContext(conf);


//Create a Java Context which is the same as the scala one under the hood
JavaSparkContext sc =JavaSparkContext.fromSparkContext(sparkc);
		 
		Broadcast<ContingencyService> broadcastContingencyService = sc.broadcast(contingencyService);
		

List<Tuple2<Integer, Map<String,String>>> datasetA = new ArrayList<Tuple2<Integer, Map<String,String>>>();      
 
IntStream.range(0, sizeA).forEach(innerIA -> 
{
	final Map<String, String> valuesI = new HashMap<>();
      variabileNomeListMA.forEach(varnameMA -> {
		valuesI.put(varnameMA, worksetIn.get(codeMatchingA).get(varnameMA).get(innerIA));
	});
      datasetA.add(new Tuple2<Integer, Map<String,String>>(innerIA,valuesI));
});

List<Tuple2<Integer, Map<String,String>>> datasetB = new ArrayList<Tuple2<Integer, Map<String,String>>>();      

IntStream.range(0, sizeB).forEach(innerIB -> 
{
	final Map<String, String> valuesI = new HashMap<>();
      variabileNomeListMB.forEach(varnameMB -> {
		valuesI.put(varnameMB, worksetIn.get(codeMatchingB).get(varnameMB).get(innerIB));
	});
      datasetB.add(new Tuple2<Integer, Map<String,String>>(innerIB,valuesI));
});
 
JavaPairRDD<Integer, Map<String, String>> datasetARDD = sc.parallelizePairs(datasetA);
JavaPairRDD<Integer, Map<String, String>> datasetBRDD = sc.parallelizePairs(datasetB);
		 	
		 	
		    final JavaPairRDD<Object, Object> cartesian =  datasetARDD.cartesian(datasetBRDD)
		    	
		    		.mapToPair(scalaTuple ->{ 
		    			final Map<String, String> valuesI =  scalaTuple._1._2;
		    			valuesI.putAll(scalaTuple._2._2);
		    			return new Tuple2<>(broadcastContingencyService.getValue().getPattern(valuesI),1);
		    				
		    		});
		    final Map<Object, Long> contengencyTable=cartesian.countByKey();
	 
		  
		contengencyTableOut.put(VARIABLE_FREQUENCY, new ArrayList<>());
		// write to worksetout
		contengencyTable.forEach((key, value) -> {
			int idx = 0;
			for (String nameMatchingVariable : nameMatchingVariables) {
				contengencyTableOut.get(nameMatchingVariable).add(String.valueOf(((String) key).charAt(idx++)));
			}
			contengencyTableOut.get(VARIABLE_FREQUENCY).add(value.toString());
		});

		rolesOut.put(codContengencyTable, new ArrayList<>(contengencyTableOut.keySet()));
	//	rolesOut.put(codContengencyIndexTable, new ArrayList<>(coupledIndexByPattern.keySet()));
		returnOut.put(EngineService.ROLES_OUT, rolesOut);

		rolesOut.keySet().forEach(code -> {
			rolesGroupOut.put(code, code);
		});
		returnOut.put(EngineService.ROLES_GROUP_OUT, rolesGroupOut);

		worksetOut.put(codContengencyTable, contengencyTableOut);
		//worksetOut.put(codContengencyIndexTable, coupledIndexByPattern);

		returnOut.put(EngineService.WORKSET_OUT, worksetOut);

		System.out.println("Spark time:" + Duration.between(start, Instant.now()).toSeconds());

		return returnOut;
	}
	
	public Map<?, ?> pRLContingencyTableCartesianProductSpark2(Long idelaborazione,
			Map<String, List<String>> ruoliVariabileNome, Map<String, Map<String, List<String>>> worksetIn,
			Map<String, String> parametriMap) throws Exception {

		 System.setProperty("hadoop.home.dir", "C:\\winutils");
		
		Instant start = Instant.now();

		final Map<String, Map<?, ?>> returnOut = new HashMap<>();
		final Map<String, Map<?, ?>> worksetOut = new HashMap<>();

		final Map<String, ArrayList<String>> contengencyTableOut = new LinkedHashMap<>();
		final Map<String, ArrayList<String>> rolesOut = new LinkedHashMap<>();
		final Map<String, String> rolesGroupOut = new HashMap<>();

		ArrayList<String> variabileNomeListMA = new ArrayList<>();
		ArrayList<String> variabileNomeListMB = new ArrayList<>();

		ArrayList<String> variabileNomeListOut = new ArrayList<>();

		////logService.save("Process Contingency Table Starting...");

		ruoliVariabileNome.get(codeMatchingA).forEach((varname) -> {
			variabileNomeListMA.add(varname);
		});
		////logService.save("Matching variables dataset A: " + variabileNomeListMA);
		ruoliVariabileNome.get(codeMatchingB).forEach((varname) -> {
			variabileNomeListMB.add(varname);
		});
		////logService.save("Matching variables dataset B: " + variabileNomeListMB);
		ruoliVariabileNome.values().forEach((list) -> {
			variabileNomeListOut.addAll(list);
		});

		String firstFiledMA = ruoliVariabileNome.get(codeMatchingA).get(0);
		String firstFiledMB = ruoliVariabileNome.get(codeMatchingB).get(0);
		int sizeA = worksetIn.get(codeMatchingA).get(firstFiledMA).size();
		int sizeB = worksetIn.get(codeMatchingB).get(firstFiledMB).size();

		try {
			contingencyService.init(parametriMap.get(params_MatchingVariables));
		} catch (Exception e) {
			////logService.save("Error parsing " + params_MatchingVariables);
			throw new Exception("Error parsing " + params_MatchingVariables);
		}
		List<String> nameMatchingVariables = new ArrayList<>();

		contingencyService.getMetricMatchingVariableVector().forEach(metricsm -> {
			contengencyTableOut.put(metricsm.getMatchingVariable(), new ArrayList<>());
			nameMatchingVariables.add(metricsm.getMatchingVariable());
		});

	 
		//final Map<String, List<String>> coupledIndexByPattern = RelaisUtility.getEmptyMapByKey(
		//		contengencyTable.keySet().stream().filter(key -> Integer.parseInt(key) > 0), PREFIX_PATTERN);

		// Create a local StreamingContext with two working thread and batch interval of
		// 1 second
		SparkConf conf = new SparkConf();
		conf.setAppName("Spark MultipleContest Test");
		conf.set("spark.driver.allowMultipleContexts", "true");
		conf.setMaster("local[*]");

		// JavaStreamingContext jssc = new JavaStreamingContext(conf,
		// Durations.seconds(1));

		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Broadcast<Map<String, Map<String, List<String>>>> broadcastWorksetIn = sc.broadcast(worksetIn);
		Broadcast<ArrayList<String>> broadcastVariabileNomeListMA = sc.broadcast(variabileNomeListMA);
		Broadcast<ArrayList<String>> broadcastVariabileNomeListMB = sc.broadcast(variabileNomeListMB);
		Broadcast<ContingencyService> broadcastContingencyService = sc.broadcast(contingencyService);
		
 
		    JavaRDD<Integer> indexesA = sc.parallelize(IntStream.range(0, sizeA).boxed().collect(Collectors.toList()));
		    JavaRDD<Integer> indexesB = sc.parallelize(IntStream.range(0, sizeB).boxed().collect(Collectors.toList()));
		
		    final JavaPairRDD<String, Integer>  cartesian =  indexesA.cartesian(indexesB).cache()
		    	
		    		.mapToPair(scalaTuple ->{ 
		    			final Map<String, String> valuesI = new HashMap<>();

		    			broadcastVariabileNomeListMA.getValue().forEach(varnameMA -> {
		    				valuesI.put(varnameMA, broadcastWorksetIn.getValue().get("X1").get(varnameMA).get(scalaTuple._1.intValue()));
		    			});
		    			broadcastVariabileNomeListMB.getValue(). forEach(varnameMB -> {
							valuesI.put(varnameMB, broadcastWorksetIn.getValue().get("X2").get(varnameMB).get(scalaTuple._2.intValue()));
						});
		    			
		    			return new Tuple2<>(broadcastContingencyService.getValue().getPattern(valuesI),1);
		    				
		    		});
		    final Map<String, Long> contengencyTable=cartesian.countByKey();
	 
		 
/*		
		sc.parallelize(IntStream.range(0, sizeA).boxed().collect(Collectors.toList())).foreach(innerIA -> {

			final Map<String, String> valuesI = new HashMap<>();
			final Map<String, Integer> contengencyTableIA = contingencyService.getEmptyContengencyTable();
		//	final Map<String, List<String>> coupledIndexByPatternIA = RelaisUtility
	//				.getEmptyMapByKey(coupledIndexByPattern.keySet().stream(), "");

			variabileNomeListMA.forEach(varnameMA -> {
				valuesI.put(varnameMA, worksetIn.get(codeMatchingA).get(varnameMA).get(innerIA));
			});

			IntStream.range(0, sizeB).forEach(innerIB -> {
				variabileNomeListMB.forEach(varnameMB -> {
					valuesI.put(varnameMB, worksetIn.get(codeMatchingB).get(varnameMB).get(innerIB));
				});

				String pattern = contingencyService.getPattern(valuesI);
				contengencyTableIA.put(pattern, contengencyTableIA.get(pattern) + 1);
	//			if (Integer.parseInt(pattern) > 0)
		//			coupledIndexByPatternIA.get(PREFIX_PATTERN + pattern).add((innerIA + 1) + ";" + (innerIB + 1)); // store
																													// no
																													// zero
																													// based

			});
			synchronized (contengencyTable) {
				contengencyTableIA.entrySet().stream().forEach(e -> contengencyTable.put(e.getKey(),
						contengencyTable.get(e.getKey()) + contengencyTableIA.get(e.getKey())));
			}
	/*		synchronized (coupledIndexByPattern) {
				coupledIndexByPatternIA.entrySet().stream().forEach(
						e -> coupledIndexByPattern.get(e.getKey()).addAll(coupledIndexByPatternIA.get(e.getKey())));
			}

		});
		*/
		contengencyTableOut.put(VARIABLE_FREQUENCY, new ArrayList<>());
		// write to worksetout
		contengencyTable.forEach((key, value) -> {
			int idx = 0;
			for (String nameMatchingVariable : nameMatchingVariables) {
				contengencyTableOut.get(nameMatchingVariable).add(String.valueOf(key.charAt(idx++)));
			}
			contengencyTableOut.get(VARIABLE_FREQUENCY).add(value.toString());
		});

		rolesOut.put(codContengencyTable, new ArrayList<>(contengencyTableOut.keySet()));
	//	rolesOut.put(codContengencyIndexTable, new ArrayList<>(coupledIndexByPattern.keySet()));
		returnOut.put(EngineService.ROLES_OUT, rolesOut);

		rolesOut.keySet().forEach(code -> {
			rolesGroupOut.put(code, code);
		});
		returnOut.put(EngineService.ROLES_GROUP_OUT, rolesGroupOut);

		worksetOut.put(codContengencyTable, contengencyTableOut);
		//worksetOut.put(codContengencyIndexTable, coupledIndexByPattern);

		returnOut.put(EngineService.WORKSET_OUT, worksetOut);

		System.out.println("Spark time:" + Duration.between(start, Instant.now()).toSeconds());

		return returnOut;
	}
	
}
