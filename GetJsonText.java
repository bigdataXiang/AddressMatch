package com.svail.chongqing;

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
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.svail.geotext.GeoQuery;
import com.svail.util.FileTool;
import com.svail.util.LongUrlToShort;
import com.svail.util.ReadJson;
import com.svail.util.Tool;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class GetJsonText {
	
	public static void main(String[] args) {
		//step-1：将所有子文件合成一个总文件
		//mergeFile("D:/百度_代码+试运行结果/baiduCrawl/","chengguo");
		
		//step-2:将合成的总文件进行去冗余处理
		//Tool.delectRedundancy("");
		
		//step-3:将数据进行地理编码处理
		//jsonAddressMatch("D:/百度_代码+试运行结果/0524/生态文明建设成果/生态文明建设成果-delectRedundancy_NullException.txt");
		
		//step-4：将地理编码处理后的数据转换成json格式
		//exportJson("D:/百度_代码+试运行结果/0524/生态文明建设项目工程/生态文明建设项目-delectRedundancy_result.txt");
		
	}
	public static void mergeFile(String path,String type){
		String[] names={"鼓楼区",
				"云龙区",
				"贾汪区",
				"泉山区",
				"铜山区",
				"丰县",
				"沛县",
				"睢宁县",
				"新沂市",
				"邳州市",
				"常州市",
				"天宁区",
				"钟楼区",
				"戚墅堰区"};
		//String path="D:/百度_代码+试运行结果/baiduCrawl/";
		String folder="";
		for(int k=0;k<names.length;k++){
			String county=names[k];
			folder=path+county+".json";
			Vector<String> file=FileTool.Load(folder, "utf-8");
			for(int i=0;i<file.size();i++){
				System.out.println(file.elementAt(i));
				FileTool.Dump(file.elementAt(i), path+type+"-Result.txt", "utf-8");
			}								
		}
		
	}
	
	public static void exportJson(String file) {
		
		Vector<String> rds = FileTool.Load(file, "UTF-8");
		
		JSONObject root = new JSONObject();
		
		root.put("type", "FeatureCollection");
		
		ArrayList<JSONObject> features = new ArrayList<JSONObject>();
		
		for (int n = 0; n < rds.size(); n ++) {
			
			/*txtc = "{ \"type\":\"Feature\", \"geometry\": {\"type\":\"Point\", \"coordinates\":[" + lng[0] + "," + lng[1] + "]},\"properties\": {\"Title\":\""+ title+"\""
				+  ",\"href\":\"" +shorturl+ "\",\"time\":\"" + jsonObjectTemp.getString("time").replace("-", "").replace(" ", "")
				+ "\",\"abstract\":\"" + abs+ "\",\"region\":\"" + Admin + "\"}}";
			*/
			/*txtc = "<TTTLE>" + title + "</TITLE>" + "<URL>" + shorturl + "</URL>" + "<ABS>" + abs + "</ABS>" + "<TIME>" + time + "</TIME>" +
				"<LNG>" + lng[0] + "</LNG>" + "<LAT>" + lng[1] + "</LAT>" + "<REGION>" + Admin + "</REGION>";*/
			
			JSONObject obj = new JSONObject();
			obj.put("type", "Feature");
			
			String ref = rds.get(n);
			
			String title=Tool.getStrByKey(ref, "<TTTLE>", "</TITLE>", "</TITLE>");
			
			JSONObject prop = new JSONObject();
			prop.put("title", title.replace("\"", "\\\""));
			
			String URL=Tool.getStrByKey(ref, "<URL>", "</URL>", "</URL>");
			prop.put("href", URL);
			
			String ABS=Tool.getStrByKey(ref, "<ABS>", "</ABS>", "</ABS>");
			prop.put("abs", ABS.replace("\"", "\\\""));

			String TIME=Tool.getStrByKey(ref, "<TIME>", "</TIME>", "</TIME>");
			prop.put("time", TIME);
			
			String REGION=Tool.getStrByKey(ref, "<REGION>", "</REGION>", "</REGION>");
			prop.put("region", REGION);
		
			obj.put("properties", prop);
			
			JSONObject geom = new JSONObject();
			double coord[] = {Double.parseDouble(Tool.getStrByKey(ref, "<LNG>", "</LNG>", "</LNG>")),
					Double.parseDouble(Tool.getStrByKey(ref, "<LAT>", "</LAT>", "</LAT>"))};
			
			// "geometry\": {\"type\":\"Point\", \"coordinates\":[" + lng[0] + "," + lng[1] + "]}
			geom.put("type", "Point");
			
			geom.put("coordinates", coord);
			
			obj.put("geometry", geom);
			System.out.println(obj);
			features.add(obj);
			
		}
		root.put("features", features);
		System.out.println(root);
		
		FileTool.Dump(root.toString(), "D:/百度_代码+试运行结果/0524/生态文明建设项目工程/生态文明建设项目-Result.json", "UTF-8");//file.replace(".txt", "")+"-json.json"
		
	}
	public static void jsonAddressMatch(String file){

		// TODO Auto-generated method stub
		String request ="http://192.168.6.9:8080/p41?f=json";
		String parameters = "&key=206DA5B15B5211E5BFE0B8CA3AF38727&queryStr=";

		boolean batch = true;
		Gson gson = new Gson();
		if (batch)
			request = "http://192.168.6.9:8080/p4b?";
		StringBuffer sb = new StringBuffer();
		int offset = 0;
		String poi="";
		int count = 0;
		Vector<String> validpois = new Vector<String>();
		
		Vector<String> pois = FileTool.Load(file, "utf-8");
		
		String txtc = "{ \"type\": \"FeatureCollection\", \"features\": [";
		
		// FileTool.Dump(txtc, file.replace(".txt", "") + "_result.json", "UTF-8");
		
		int cnt = 0;
		for (int k = 0; k < pois.size(); k++) {
			if (batch) {
				String poi1=pois.elementAt(k).replace("},", "}");
				JSONObject jsonObject =JSONObject.fromObject(poi1);
				String address=(String) jsonObject.get("Title");
				validpois.add(poi1);
				count ++;
				sb.append(address).append("\n");
				if (((count == 1000) ||  k == pois.size() - 1)) {

					String urlParameters = sb.toString();
					System.out.print("批量处理开始：");
					count = 0;
					byte[] postData;
					try {
						postData = (parameters + java.net.URLEncoder.encode(urlParameters,"UTF-8")).getBytes(Charset.forName("UTF-8"));
						int postDataLength = postData.length;
				            
						URL url = new URL(request);
						//System.out.println(request + urlParameters);
						HttpURLConnection cox = (HttpURLConnection) url.openConnection();
						cox.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; rv:11.0) like Gecko");
						cox.setDoOutput(true);
						cox.setDoInput(true);
						cox.setInstanceFollowRedirects(false);
						cox.setRequestMethod("POST");
						// cox.setRequestProperty("Accept-Encoding", "gzip");  
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
									reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(bis), "utf-8"));
								} else {
									// 取前两个字节
									reader = new BufferedReader(new InputStreamReader(bis, "utf-8"));
								}
								
								// 创建一个JsonParser
								JsonParser parser = new JsonParser();
								String txt ="";
								//通过JsonParser对象可以把json格式的字符串解析成一个JsonElement对象
								try {
									
									txt = reader.readLine();
									if (txt == null) {
										System.out.println("txt为null！");
										for(int i=0;i<validpois.size();i++){
											FileTool.Dump(validpois.get(i), file.replace(".txt", "") + "_NullException.txt", "UTF-8");
											
										}
									}
									else {
										int index1=txt .indexOf("chinesename");
										String index3=",}";
										if(index1!=-1&&index3!=null)
											txt =txt .replace(",}", "}");
										 JsonElement el = parser.parse(txt);
										// JsonElement el = parser.parse(tesobj.toString());
										//把JsonElement对象转换成JsonObject
										JsonObject jsonObj = null;
										if(el.isJsonObject())
										{
											jsonObj = el.getAsJsonObject();
											//System.out.println(jsonObj);
  										    GeoQuery gq = gson.fromJson(jsonObj, GeoQuery.class);
											String lnglat = "";
											String Admin="";
											if (gq != null && gq.getResult() != null && gq.getResult().size() > 0)
											{
												System.out.println("这批数据没有问题！");
												
												for (int m = 0; m < gq.getResult().size(); m ++)
												{
													if (gq.getResult().get(m) != null && gq.getResult().get(m).getLocation() != null)
													{
														if(gq.getResult().get(m).getLocation().getRegion()!=null)
														Admin=(gq.getResult().get(m).getLocation().getRegion().getProvince()+gq.getResult().get(m).getLocation().getRegion().getCity()+gq.getResult().get(m).getLocation().getRegion().getCounty()+gq.getResult().get(m).getLocation().getRegion().getTown()).replace("null", "");
														else
															Admin="暂无";
														lnglat =gq.getResult().get(m).getLocation().getLng() + "," + gq.getResult().get(m).getLocation().getLat();
														double[] lng=new double[2];
														lng[0]=gq.getResult().get(m).getLocation().getLng();
														lng[1]=gq.getResult().get(m).getLocation().getLat();
														String poitemp=validpois.elementAt(m);
														
														
														JSONObject jsonObjectTemp =JSONObject.fromObject(poitemp);
														String longurl=jsonObjectTemp.getString("href");
														String shorturl=LongUrlToShort.createShortUrl(longurl);
														String title=jsonObjectTemp.getString("Title").replace("\"", "");
														String abs=jsonObjectTemp.getString("abstract").replace("-", "").replace("\"", "");
														String time = jsonObjectTemp.getString("time").replace("-", "").replace(" ", "");
														/*txtc = "{ \"type\":\"Feature\", \"geometry\": {\"type\":\"Point\", \"coordinates\":[" + lng[0] + "," + lng[1] + "]},\"properties\": {\"Title\":\""+ title+"\""
															+  ",\"href\":\"" +shorturl+ "\",\"time\":\"" + jsonObjectTemp.getString("time").replace("-", "").replace(" ", "")
															+ "\",\"abstract\":\"" + abs+ "\",\"region\":\"" + Admin + "\"}}";
														*/
														txtc = "<TTTLE>" + title + "</TITLE>" + "<URL>" + shorturl + "</URL>" + "<ABS>" + abs + "</ABS>" + "<TIME>" + time + "</TIME>" +
																"<LNG>" + lng[0] + "</LNG>" + "<LAT>" + lng[1] + "</LAT>" + "<REGION>" + Admin + "</REGION>";
														
														FileTool.Dump(txtc, file.replace(".txt", "") + "_result.txt", "UTF-8");
														
														/*if (cnt == 0)
														{
															FileTool.Dump(txtc, file.replace(".txt", "") + "_result.json", "UTF-8");
															cnt = 1;
														}
														else
														{
															FileTool.Dump("," + txtc, file.replace(".txt", "") + "_result.json", "UTF-8");
															
														}*/
													}
													else
													{
														FileTool.Dump(validpois.elementAt(m), file.replace(".txt", "") + "_nonPostalCoor1.txt", "UTF-8");
														//System.out.print(validpois.elementAt(m));
													}
												}
											}
										}
									}

								}catch (JsonSyntaxException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
									System.out.println(e.getMessage());
									System.out.println("存在JsonSyntaxException异常！");
									for(int i=0;i<validpois.size();i++){
										FileTool.Dump(validpois.get(i), file.replace(".txt", "") + "_JsonSyntax.txt", "UTF-8");
										
									}
									FileTool.Dump(txt, file.replace(".txt", "") + "_JsonSyntaxException.txt", "UTF-8");
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
					}catch(NullPointerException e){
						e.printStackTrace();
						FileTool.Dump(poi, file.replace(".txt", "") + "_PostalNull1.txt", "UTF-8");
					}

					validpois.clear();
					sb.setLength(0);
				
					
				}
			}
			
		}

		
		// txtc = "]};";
				
		// FileTool.Dump(txtc, file.replace(".txt", "") + "_result.json", "UTF-8");
		//jsonObjectTemp.toString()
		//System.out.println(poi);
		
		//System.out.println(JsonContext);
	
	}
	
		


}
