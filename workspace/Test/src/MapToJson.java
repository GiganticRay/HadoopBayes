import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;


public class MapToJson
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("key1", 1);
		map.put("key2", 2);
		String mapJson = JSON.toJSONString(map);	// 序列化
		
		Map<String, Integer> map1 = JSON.parseObject(mapJson, new TypeReference<Map<String, Integer>>(){});	// 反序列化
		System.out.println(mapJson);
		System.out.println(map1.get("key2"));
	}
	

}
