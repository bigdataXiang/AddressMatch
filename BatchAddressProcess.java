package com.svail.batchprocessing;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.svail.geotext.GeoQuery;
import com.svail.geotext.Result;
import com.svail.geotext.Within;
import com.svail.geotext.Location;
import com.svail.geotext.Region;
import com.svail.util.FileTool;
import com.svail.util.HTMLTool;

public class BatchAddressProcess{
	 public static void main(String argv[]) throws Exception{
		 System.out.println("开始批量处理数据:");
		 processCSV("D:/Test/fang_rentout0119.txt");
		// parseLngLat("北京地理所");
		
	 }
	 public static void processCSV(String file) throws UnsupportedEncodingException
		{
			
			Vector<String> pois = FileTool.Load(file, "utf-8");
			String request ="http://192.168.6.9:8080/p41?f=json";
			//http://geocode.svail.com:8080/p41?f=xml
			//http://192.168.6.9:8080/p41?f=json
			String parameters = "&within="
				+ java.net.URLEncoder.encode("北京市", "UTF-8")
				+ "&key=206DA5B15B5211E5BFE0B8CA3AF38727&queryStr=";

			boolean batch = true;
			Gson gson = new Gson();
			if (batch)
				request = "http://192.168.6.9:8080/p4b?";
			StringBuffer sb = new StringBuffer();
			int offset = 0;
			String poi="";
			int count = 0;
			Vector<String> validpois = new Vector<String>();
			for (int n = 0; n < pois.size(); n ++) {
				if (batch) {
					String rs = pois.get(n);
					String addr0=getStrByKey(rs,"<LOCATION>","</LOCATION>");
					String addr1=getStrByKey(rs,"<SALE_ADDRESS>","</SALE_ADDRESS>");
					String addr2=getStrByKey(rs,"<COMMUNITY>","</COMMUNITY>");
					String addr3=getStrByKey(rs,"<ADDRESS>","</ADDRESS>");
					String addr4=getStrByKey(rs,"<TITLE>","</TITLE>");
					if(addr3.indexOf("固安固安")!=-1||addr3.indexOf("廊坊廊坊")!=-1||addr3.indexOf("三河三河")!=-1||addr3.indexOf("香河香河")!=-1||addr3.indexOf("涿州涿州")!=-1||addr3.indexOf("永清永清")!=-1||addr3.indexOf("燕郊燕郊")!=-1||addr3.indexOf("投资投资")!=-1||addr3.indexOf("武清武清")!=-1)
					{
						FileTool.Dump(rs, file.replace(".txt", "") + "_1_nonresult.txt", "UTF-8");
						
						if (n < pois.size() - 1)
							continue;
					}
					else
					{
						validpois.add(rs);
						count ++;
						//System.out.println(addr2+addr3);
						sb.append(addr2+addr3).append("\n");//"北京"+
					}
					
					if ((count == 10000) ||  n == pois.size() - 1) {
						String urlParameters = sb.toString();
						//System.out.print(urlParameters);
						count = 0;
						byte[] postData;
						try {
							postData = (parameters + java.net.URLEncoder.encode(urlParameters,
									"UTF-8")).getBytes(Charset.forName("UTF-8"));
							int postDataLength = postData.length;
					            
							URL url = new URL(request);
							//System.out.println(request + urlParameters);
							HttpURLConnection cox = (HttpURLConnection) url
									.openConnection();
							cox.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; rv:11.0) like Gecko");
							cox.setDoOutput(true);
							cox.setDoInput(true);
							cox.setInstanceFollowRedirects(false);
							cox.setRequestMethod("POST");
							cox.setRequestProperty("Accept-Encoding", "gzip");  
							cox.setRequestProperty("Content-Type",
									"application/x-www-form-urlencoded");
							cox.setRequestProperty("charset", "utf-8");
							cox.setRequestProperty("Content-Length",
									Integer.toString(postDataLength));
							cox.setUseCaches(false);
							
							try (DataOutputStream wr = new DataOutputStream(
									cox.getOutputStream())) {
								
								wr.write(postData);
								
								InputStream is = cox.getInputStream();
								if (is != null) {
									byte[] header = new byte[2];
									BufferedInputStream bis = new BufferedInputStream(is);
									bis.mark(2);
									int result = bis.read(header);

									// reset输入流到开始位置
									bis.reset();
									BufferedReader reader = null;
									// 判断是否是GZIP格式
									int ss = (header[0] & 0xff) | ((header[1] & 0xff) << 8);
									if (result != -1 && ss == GZIPInputStream.GZIP_MAGIC) {
										// System.out.println("为数据压缩格式...");
										reader = new BufferedReader(new InputStreamReader(
												new GZIPInputStream(bis), "utf-8"));
									} else {
										// 取前两个字节
										reader = new BufferedReader(new InputStreamReader(bis, "utf-8"));
									}
									
									// 创建一个JsonParser
									JsonParser parser = new JsonParser();
							
									//通过JsonParser对象可以把json格式的字符串解析成一个JsonElement对象
									try {
										/* GeoQuery tesobj = new GeoQuery();
										List<Double> box = new ArrayList<Double>();
										box.add(123.1);
										box.add(123.56);
										
										String text = "{\"status\":\"OK\",\"within\":{\"name\":\"北京市\",\"center_lng\":116.3847599,\"center_lat\":39.90230163},\"total\":1,\"result\":[{\"status\":\"OK\",\"query_string\":\"北京大兴,亦庄\",\"nlp_status\":\"地址完全匹配\",\"location\":{\"matched\":\"亦庄\",\"lng\":116.475885,\"lat\":39.799066,\"resolution\":\"区域中心定位\",\"region\":{\"province\":\"北京市\",\"county\":\"大兴区\"},\"geocode\":\"A9B3A6F9581\",\"box\":[123,1234,1234.01]}}]}";	
										
										Within within = new Within();
										within.setName("北京市");
										within.setCenter_lng(116.3847599);
										within.setCenter_lat(39.90230163);
										Region region = new Region();
										region.setProvince("北京市");
										region.setCounty("大兴区");
										Location loc = new Location("亦庄", 116.475885, 39.799066, "区域中心定位",  region, box, "test");
										// {\"matched\":\"亦庄\",\"lng\":116.475885,\"lat\":39.799066,\"resolution\":\"区域中心定位\",\"region\":{\"province\":\"北京市\",\"county\":\"大兴区\"},\"geocode\":\"A9B3A6F9581\",\"box\":[123,1234,1234.01]}
										Result rest = new Result("OK", "北京大兴,亦庄", "地址完全匹配", "test", loc);
										
										rest.setLocation(loc);
										
										rest.setNlp_status("地址完全匹配");
										rest.setQuery_string("北京大兴亦庄");
										rest.setSource("test");
										rest.setStatus("OK");
										tesobj.setStatus("OK");
										tesobj.setTotal(1);
										List<Result> results = new ArrayList<Result>();
										results.add(rest);
										
										tesobj.setResult(results);
										*/
										JsonElement el = parser.parse(reader.readLine());
										// String text = "{\"status\":\"OK\",\"within\":{\"name\":\"北京市\",\"center_lng\":116.3847599,\"center_lat\":39.90230163},\"total\":1,\"result\":[{\"status\":\"OK\",\"query_string\":\"北京大兴,亦庄\",\"nlp_status\":\"地址完全匹配\",\"location\":{\"matched\":\"亦庄\",\"lng\":116.475885,\"lat\":39.799066,\"resolution\":\"区域中心定位\",\"region\":{\"province\":\"北京市\",\"county\":\"大兴区\"},\"geocode\":\"A9B3A6F9581\",\"box\":[123,1234,1234.01]}}]}";	
										// JsonElement el = parser.parse(tesobj.toString());
										//把JsonElement对象转换成JsonObject
										JsonObject jsonObj = null;
										if(el.isJsonObject())
										{
											jsonObj = el.getAsJsonObject();
											System.out.println(jsonObj);
   										    GeoQuery gq = gson.fromJson(jsonObj, GeoQuery.class);
											String lnglat = "";
											if (gq != null && gq.getResult() != null && gq.getResult().size() > 0)
											{
												//System.out.println(gq.getResult());
												//System.out.println(gq.getResult().size());
												for (int m = 0; m < gq.getResult().size(); m ++)
												{
													if (gq.getResult().get(m) != null && gq.getResult().get(m).getLocation() != null)
													{
														lnglat = "<COORDINATE>" + gq.getResult().get(m).getLocation().getLng() + ";" + gq.getResult().get(m).getLocation().getLat()+"</COORDINATE>";
														String poitemp= validpois.elementAt(m);
														poi=poitemp.substring(0, poitemp.indexOf("<POI>")+"<POI>".length())+lnglat+poitemp.substring(poitemp.indexOf("<POI>")+"<POI>".length(),poitemp.indexOf("</POI>")+"</POI>".length());
														FileTool.Dump(poi, file.replace(".txt", "") + "_result.txt", "UTF-8");
														System.out.println(poi);
														
													}
													else
													{
														FileTool.Dump(validpois.elementAt(m), file.replace(".txt", "") + "_nonresult.txt", "UTF-8");
														System.out.print(validpois.elementAt(m));
													}
												}
											}
										}

									}catch (JsonSyntaxException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}

								}
							}

						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (MalformedURLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						validpois.clear();
						sb.setLength(0);
					}

				} else {

					try {
						
						String xml ="";
						String rs = pois.get(n);
						String addr0=getStrByKey(rs,"<LOCATION>","</LOCATION>");
						String addr1=getStrByKey(rs,"<SALE_ADDRESS>","</SALE_ADDRESS>");
						String addr2=getStrByKey(rs,"<COMMUNITY>","</COMMUNITY>");
						String addr3=getStrByKey(rs,"<ADDRESS>","</ADDRESS>");
						String addr4=getStrByKey(rs,"<TITLE>","</TITLE>");
						if(addr3.indexOf("固安固安")!=-1||addr3.indexOf("廊坊廊坊")!=-1||addr3.indexOf("三河三河")!=-1||addr3.indexOf("香河香河")!=-1||addr3.indexOf("涿州涿州")!=-1||addr3.indexOf("永清永清")!=-1||addr3.indexOf("燕郊燕郊")!=-1||addr3.indexOf("投资投资")!=-1||addr3.indexOf("武清武清")!=-1)
							FileTool.Dump(rs, file.replace(".txt", "") + "_1_nonresult.txt", "UTF-8");	
						else
							xml = parseLngLat(addr2+addr3);//"北京"+
						poi=rs.substring(0, rs.indexOf("<POI>")+"<POI>".length())+xml+rs.substring(rs.indexOf("<POI>")+"<POI>".length(),rs.indexOf("</POI>")+"</POI>".length());
						if (xml != null)
						{
							FileTool.Dump(poi,file.replace(".txt", "") + "_result.txt", "UTF-8");
							//System.out.println("Line " + n + " [" + xml + "]");							
						}
						else
							FileTool.Dump(poi,file.replace(".txt", "") + "_nonresult.txt", "UTF-8");
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}				
				}
			}
			
		}

	public static String getStrByKey(String sContent, String sStart, String sEnd) {
		String sOut ="";
		int fromIndex = 0;
		int iBegin = 0;
		int iEnd = 0;
		int iStart=sContent.indexOf("</POI>");
		if (iStart < 0) {
		  return null;
		  }
		for (int i = 0; i < iStart; i++) {
		  // 找出某位置，并找出该位置后的最近的一个匹配
		  iBegin = sContent.indexOf(sStart, fromIndex);
		  if (iBegin >= 0) 
		  {
		    iEnd = sContent.indexOf(sEnd, iBegin + sStart.length());
		    if (iEnd <= iBegin)
		    {
		      return null;
		    }
		  }
		  else 
		  {
				return sOut;
		  }
          if (iEnd > 0&&iEnd!=iBegin + sStart.length())
          {
		   sOut += sContent.substring(iBegin + sStart.length(), iEnd);
		  }
          else
        	  return null;
		  if (iEnd > 0) 
		  {
		   fromIndex = iEnd + sEnd.length();
		  }
		}
		  return sOut;
	}
	
	public static String parseLngLat(String query) throws UnsupportedEncodingException{
		String request = "http://192.168.6.9:8080/p41?f=json";
		String parameters = "&within="
			+ java.net.URLEncoder.encode("北京市", "UTF-8")
			+ "&key=206DA5B15B5211E5BFE0B8CA3AF38727&queryStr=";

		Gson gson = new Gson();
		String lnglat = "";
		String uri = null;
		try {
			uri = request + parameters+ java.net.URLEncoder.encode(query, "UTF-8");
			String xml = HTMLTool.fetchURL(uri, "UTF-8", "post");
			
			if (xml != null)
			{
				// 创建一个JsonParser
				JsonParser parser = new JsonParser();
		
				//通过JsonParser对象可以把json格式的字符串解析成一个JsonElement对象
				try {
					JsonElement el = parser.parse(xml);

					//把JsonElement对象转换成JsonObject
					JsonObject jsonObj = null;
					if(el.isJsonObject())
					{
						jsonObj = el.getAsJsonObject();
						GeoQuery gq = gson.fromJson(jsonObj, GeoQuery.class);
						
						if (gq != null && gq.getResult() != null && gq.getResult().size() > 0 && gq.getResult().get(0).getLocation() != null)
						{
							lnglat ="<COORDINATE>"+gq.getResult().get(0).getLocation().getLng() + ";" + gq.getResult().get(0).getLocation().getLat()+ "</COORDINATE>" ;
							
							
						}
					}
					
					
				}catch (JsonSyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
			return lnglat;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		return null;
	}
	
}
