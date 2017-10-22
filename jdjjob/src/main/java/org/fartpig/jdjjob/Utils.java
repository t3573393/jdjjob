package org.fartpig.jdjjob;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Administrator
 *
 */
public class Utils {

	/**
	 * merge the mergeMap to the originalMap
	 * 
	 * @param originalMap
	 * @param mergeMap
	 * @return
	 */
	public static Map<String, Object> mergeMaps(Map<String, Object> originalMap, Map<String, Object> mergeMap) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.putAll(originalMap);
		result.putAll(mergeMap);
		return result;
	}

}
