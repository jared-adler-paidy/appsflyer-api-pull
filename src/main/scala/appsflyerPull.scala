import org.apache.spark.sql.SparkSession
import sttp.client3._
import sttp.model._
import sttp.model.Uri
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

object appsflyerPull {

  def main(args: Array[String]) {


    def createSparkSession(): SparkSession = {
      val builder = SparkSession.builder
      builder.appName("ymir")
      builder.getOrCreate()

    }


    val spark: SparkSession = createSparkSession()
    spark.sparkContext.setLogLevel("WARN")

    import spark.sqlContext.implicits._

    val backend = HttpClientSyncBackend()

    val appID = "com.paidy.paidy"
    val reportName = "installs_report"


    val appsflyerSource = "https://hq.appsflyer.com/export/%s/%s/v5".format(appID, reportName)

    val params =
      Map(
        "api_token" -> "hello",
        "from" -> "2022-06-01 00:00:00",
        "to" -> "2022-06-02 00:00:00",
        "maximum_rows" -> "1000000"
      )

    val endpointWithParams: Uri = uri"$appsflyerSource?$params"

    println(endpointWithParams)

    val response: Identity[Response[Either[String, String]]] =
      basicRequest
      .followRedirects(true)
      .get(endpointWithParams)
      .response(asString)
      .send(backend)

    val responseBody = response.body

    val responseString = responseBody match{
      case Right(responseBody) => responseBody
      case Left(_) => "error"
    }

    val responseLines = responseString.lines.toArray

    val reportCols = responseLines.head.toString.split(",").slice(0, 24)

    val appsflyerSchema =
      StructType(reportCols.map(x => StructField(x.replace(" ", "").toLowerCase,StringType, nullable = true)))

    val csvBody = responseLines.tail.map(x => x.toString) //.replace("%20", " ")
    val csvRDD = spark.sparkContext.parallelize(csvBody).map(x => x.split(",").slice(0, 24))

    val frame = spark.sqlContext.createDataFrame(csvRDD.map(x => Row.fromSeq(x)), appsflyerSchema)



    //frame.show

    //frame.printSchema

    frame.write.parquet("/Users/jaredadler/downloads/sampleappsflyer.parquet")

/*
    val responseX = """Attributed Touch Type,Attributed Touch Time,Install Time,Event Time,Event Name,Event Value,Event Revenue,Event Revenue Currency,Event Revenue USD,Event Source,Is Receipt Validated,Partner,Media Source,Channel,Keywords,Campaign,Campaign ID,Adset,Adset ID,Ad,Ad ID,Ad Type,Site ID,Sub Site ID,Sub Param 1,Sub Param 2,Sub Param 3,Sub Param 4,Sub Param 5,Cost Model,Cost Value,Cost Currency,Contributor 1 Partner,Contributor 1 Media Source,Contributor 1 Campaign,Contributor 1 Touch Type,Contributor 1 Touch Time,Contributor 2 Partner,Contributor 2 Media Source,Contributor 2 Campaign,Contributor 2 Touch Type,Contributor 2 Touch Time,Contributor 3 Partner,Contributor 3 Media Source,Contributor 3 Campaign,Contributor 3 Touch Type,Contributor 3 Touch Time,Region,Country Code,State,City,Postal Code,DMA,IP,WIFI,Operator,Carrier,Language,AppsFlyer ID,Advertising ID,IDFA,Android ID,Customer User ID,IMEI,IDFV,Platform,Device Type,OS Version,App Version,SDK Version,App ID,App Name,Bundle ID,Is Retargeting,Retargeting Conversion Type,Attribution Lookback,Reengagement Window,Is Primary Attribution,User Agent,HTTP Referrer,Original URL
click,2022-06-01 01:03:16,2022-06-01%2001:03:32,2022-06-01%2001:03:32,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,34,Onomichi,722-0073,392008,153.171.233.129,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654045410739-8560122302309823614,27f8ad65-6884-47c5-868b-4c9ab655680c,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20SC-51A%20Build/SP1A.210812.016),,%0Aimpression,2022-05-31%2000:00:00,2022-06-01%2001:02:57,2022-06-01%2001:02:57,install,,,,,SDK,,,bytedanceglobal_int,TikTok,,restricted,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,08,Hitachinaka,312-0045,392001,126.115.4.82,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654045374793-896556757031558420,90a1143e-3142-406c-81f8-20ef4edebcf2,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20SO-05K%20Build/52.1.B.0.266),,%0Aclick,2022-06-01%2001:01:15,2022-06-01%2001:01:47,2022-06-01%2001:01:47,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,01,Ishikari,061-0207,392006,49.97.26.54,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654045304252-5108494066409475557,5d3a53a9-3c53-49b4-97e2-083b8f7b5de9,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20SC-56B%20Build/RP1A.200720.012),,%0Aclick,2022-06-01%2001:00:44,2022-06-01%2001:01:24,2022-06-01%2001:01:24,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,13,Suginami,166-0013,392001,106.128.157.236,false,KDDI,KDDI,日本語,1654045280582-1623002653513392439,f6e55dde-94d9-4a1c-8398-20953e23565f,,,,,,android,,9,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%209;%20SCV36%20Build/PPR1.180610.011),,%0Aclick,2022-06-01%2001:00:18,2022-06-01%2001:00:50,2022-06-01%2001:00:50,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,27,Takatsuki,569-1115,392002,119.228.115.1,true,Y!mobile,Y!mobile,日本語,1654045248382-4515361212707198940,2c63c369-4d52-4b92-b42f-9bbd81dd1f6e,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20A101OP%20Build/RKQ1.201217.002),https:/my.paidy.com/home,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:59:44,2022-06-01%2001:00:24,2022-06-01%2001:00:24,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,13,Adachi,123-0861,392001,36.14.61.201,true,au,KDDI,日本語,1654045222410-2178978549294039979,83e45c5c-a8b1-4cfa-aaff-0fedb34cf0bb,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20SOV41%20Build/55.2.C.3.21),https:/my.paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:56:45,2022-06-01%2000:58:00,2022-06-01%2000:58:00,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V1_Install_And,11563397199,And_USP,125859267720,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,26,Miyazu,629-2251,392002,49.98.244.55,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654045076222-3216347254155007514,1e02589a-0902-4d1e-bd1a-17318bc62db8,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20F-51B%20Build/V11RD61C),,%0Aclick,2022-06-01%2000:54:21,2022-06-01%2000:56:35,2022-06-01%2000:56:35,install,,,,,SDK,,Adways,googleadwords_int,ACI_Youtube,,Adways__x0000K4L_V3_forVideo_And,16103408041,And_Forvideo_V3,142523503864,,,ClickToDownload,YouTubeVideos,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,28,Kobe,653-0801,392002,60.101.102.163,true,Y!mobile,Y!mobile,日本語,1654044992388-500853407314766729,7d10f5cb-cd22-4338-99cb-4942bff5a108,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20Pixel%204a%20Build/SP2A.220505.002),,%0Aclick,2022-06-01%2000:52:58,2022-06-01%2000:54:18,2022-06-01%2000:54:18,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,14,Yamato,242-0017,392001,126.78.248.255,true,KDDI,KDDI,日本語,1654044854961-6812758981216659648,bd250c03-ddc2-4983-afdc-fc18f00bc30d,,,,,,android,,9,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%209;%20SHV40%20Build/S3040),,%0Aclick,2022-06-01%2000:52:14,2022-06-01%2000:54:02,202-06-01%2000:54:02,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,14,Atsugi,243-0817,392001,220.216.78.99,true,Y!mobile,Y!mobile,日本語,1654044840249-8545926340907111848,098b3de8-da26-4519-9138-1ff0ae173979,,,,,,android,,9,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%209;%20S4-KC%20Build/2.240DE.0205.a),https:/my.paidy.com/home,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aimpression,2022-05-31%2022:20:19,2022-06-01%2000:53:46,2022-06-01%2000:53:46,install,,,,,SDK,,adways,Twitter,Twitter,,Adways__定常_Paidy_And_CPC_BR_新規_TW_JP___001,hk3s8,m4ejt,m4ejt,1513532332866691076,1513532332866691076,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,14,Yokohama,234-0054,392001,219.124.252.26,true,KDDI,KDDI,日本語,1654044825956-6573781523245585375,33741db5-f1e5-4399-8d58-a2444ea07bd4,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,1d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20SCG11%20Build/SP1A.210812.016),,%0Aclick,2022-06-01%2000:51:32,2022-06-01%2000:52:16,2022-06-01%2000:52:16,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,13,Mitaka,181-0004,392001,133.106.44.2,false,Rakuten,Rakuten,日本語,1654044733647-2846566271628191261,7d104163-5430-433f-8d47-594f301359ea,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20CPH2013%20Build/RKQ1.200903.002),,%0Aclick,2022-05-30%2012:23:23,2022-06-01%2000:50:59,2022-06-012000:50:59,install,,,,,SDK,,Adways,bytedanceglobal_int,Pangle,,Adways__AF_【Pangle】Paidy_And___001,1725078960644113,【Pangle】And_動画_後払い_LC,1725089089688578,000000_0015sec_220201174516.mp4,1725341239506977,50,900000000,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,28,Suzuka,513-0001,392002,49.96.48.54,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654044657186-8678234419211188497,01889fa4-4b60-482b-8a91-45109ff5807c,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20SO-52B%20Build/62.1.B.0.463),,https:/app.appsflyer.com/com.paidy.paidy%3Fpid=bytedanceglobal_int&af_siteid=900000000&c=Adways__AF_%25E3%2580%2590Pangle%25E3%2580%2591Paidy_And___001&af_channel=Pangle&af_c_id=1725078960644113&af_adset=%25E3%2580%2590Pangle%25E3%2580%2591And_%25E5%258B%2595%25E7%2594%25BB_%25E5%25BE%258C%25E6%2589%2595%25E3%2581%2584_LC&af_adset_id=1725089089688578&af_ad=000000_0015sec_220201174516.mp4&af_ad_id=1725341239506977&af_ad_type=50&af_click_lookback=7d&clickid=E.C.P.CroBSD4mCBZaqJCxMS-CP-TtYQuqHTqTxbdGbu19ESwpfbamJPE0xw6ctRcitb3FeGv0f6MO4VuYrwZO4wDPCTpAS6iZOJG75Ix6Cy2D6qIeMUgk6Qsr1furABpAzwJHx5WnGOvN_XdBHq38PEqov3ZLLEZe9hmKg0QmHPwFOiXOTtod35H_BHN20PqkzTqA2qSQJzfpfKLZz7CsOsJs0iFuta8ipXTdzaUEEt8cy_lZxgOr2IWSLJo36Xn9EgR2Mi4wGiAFgxIlF2SQ-IPKVZ1oPKAoxQ98YekDDibQcgQ3vCRqCA&advertising_id=01889fa4-4b60-482b-8a91-45109ff5807c&idfa=&os=0&af_ip=49.97.22.177&af_ua=Mozilla%252F5.0+%2528Linux%253B+Android+12%253B+SO-52B+Build%252F62.1.B.0.463%253B+wv%2529+AppleWebKit%252F537.36+%2528KHTML%252C+like+Gecko%2529+Version%252F4.0+Chrome%252F101.0.4951.61+Mobile+Safari%252F537.36&af_lang=ja&redirect=false&af_prt=Adways&toutiaohaiwai=1%0Aclick,2022-06-01%2000:50:01,2022-06-01%2000:50:24,2022-06-01%2000:50:24,install,,,,,SDK,,,website,smart_banner,,top,,B,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,02,Tohoku,039-2648,None,14.9.85.225,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654044620769-8488352744451160069,c3946f3b-aebc-4db2-884c-282bd8dcb38e,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20SH-03K%20Build/S6220),https:/faq.paidy.com/%25E8%25AB%258B%25E6%25B1%2582%25E5%2586%2585%25E5%25AE%25B9-%25E5%2588%25A9%25E7%2594%25A8%25E7%258A%25B6%25E6%25B3%2581-%25E6%2594%25AF%25E6%2589%2595%25E6%2596%25B9%25E6%25B3%2595%25E3%2581%25AE%25E7%25A2%25BA%25E8%25AA%258D%25E3%2581%25AB%25E3%2581%25A4%25E3%2581%2584%25E3%2581%25A6,https:/paidy.onelink.me/GTiS%3Fcreative_id=4fb4ce4f-af53-485c-af67-16b315ca5c6c&af_banner=true&af_channel=smart_banner&pid=website&c=top&af_adset=B&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:47:36,2022-06-01%2000:48:35,2022-06-01%2000:48:35,install,,,,,SDK,,Adways,googleadwords_int,ACI_Youtube,,Adways__x0000K4L_V3_forVideo_And,16103408041,And_Forvideo_V3,142523503864,,,ClickToDownload,YouTubeVideos,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,27,Osaka,547-0026,392002,158.201.248.188,true,LINEMO,LINEMO,日本語,1654044513881-5037156815927060554,c50420cc-d3e9-41ba-8c65-98fc9a9a2e5e,,,,,,android,,8.1.0,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%208.1.0;%20CPH1851%20Build/OPM1.171019.026),,%0Aclick,2022-06-01%2000:43:30,2022-06-01%2000:44:23,2022-06-01%2000:44:23,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,27,Matsubara,580-0031,392002,49.250.61.57,true,KDDI,KDDI,日本語,1654044259151-3944978480603359322,3beded8-b5ca-4544-b09d-598631df27a4,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20SCV49%20Build/RP1A.200720.012),,%0Aclick,2022-06-01%2000:42:47,2022-06-01%2000:43:02,2022-06-01%2000:43:02,install,,,,,SDK,,Adways,googleadwords_int,ACI_Display,,Adways__x0000K4L_V1_Install_And,11563397199,And_USP,125859267720,,,ClickToDownload,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,46,Minamikyushu,897-0202,392013,49.104.33.163,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654044177423-3112753313589140633,3d82ae8e-36d3-4f71-913b-1bf240931be2,,,,,,android,,9,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%209;%20SC-02M%20Build/PPR1.180610.011),,%0Aclick,2022-05-31%2001:46:46,2022-06-01%2000:40:18,2022-06-01%2000:40:18,install,,,,,SDK,,Adways,googleadwords_int,ACI_Youtube,,Adways__x0000K4L_V3_forVideo_And,16103408041,And_Forvideo_V3,142523503864,,,ClickToDownload,YouTubeVideos,,,,,,engaged_view,,,,,,,,,,,,,,,,,,,AS,JP,04,Sendai,981-8004,392007,111.104.208.181,true,KDDI,KDDI,日本語,1654044017148-8063143715742396410,815a27f2-dbe1-4456-b136-b821a68968f4,,,,,,android,,12,2.9.17,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20SCG13%20Build/SP1A.210812.016),,%0Aimpression,2022-06-01%2000:33:25,2022-06-01%2000:40:17,2022-06-01%2000:40:17,install,,,,,SDK,,Adways,googleadwords_int,ACI_Display,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,25,Konan,520-3223,392002,106.146.105.35,false,KDDI,KDDI,日本語,1654044014516-368455473984612016,f1461e1b-f57a-4b98-a6b4-0a22ad732702,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,1d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20SCG06%20Build/SP1A.210812.016),,%0Aclick,2022-06-01%2000:38:52,2022-06-01%2000:39:13,2022-06-01%2000:39:13,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,13,Itabashi,174-0043,392001,106.72.142.193,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654043950734-596040652452892834,5e4c277d-9b52-4c00-bdf9-0238723608b6,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20SH-04L%20Build/SC030),,%0Aclick,2022-06-01%2000:15:17,2022-06-01%2000:37:13,2022-06-01%2000:37:13,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,14,Chigasaki,253-0043,392001,49.98.220.97,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654043831188-1517831661435340035,e761b8ad-d42e-41dd-afc0-401a2f8ed412,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20SH-53A%20Build/S2280),,%0Aclick,2022-06-01%2000:02:21,2022-06-01%2000:32:44,2022-06-01%2000:32:44,install,,,,,SDK,,Adways,googleadwords_int,ACI_Display,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,11,Urawa,330-0062,392001,126.194.104.115,true,Y!mobile,Y!mobile,日本語,1654043564220-6849486540871306432,3e458e58-d7ca-4713-b53e-015293937b6e,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20MAR-LX2J%20Build/HUAWEIMAR-L02J),,%0Aclick,2022-06-01%2000:29:42,2022-06-01%2000:32:36,2022-0-01%2000:32:36,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,21,Seki,501-3923,392003,119.245.18.61,true,KDDI,KDDI,日本語,1654043556687-2396245849777226185,a71af6b4-fde9-4dd3-bb12-93a2cf02e711,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20OPG02%20Build/SKQ1.210216.001),,%0Aclick,2022-05-28%2005:03:15,2022-06-01%2000:32:00,2022-06-01%2000:32:00,install,,,,,SDK,,Adways,googleadwords_int,ACI_Display,,Adways__x0000K4L_V1_Install_And,11563397199,And_USP,125859267720,,,ClickToDownload,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,23,Nagoya,462-0006,392003,49.97.27.152,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654043516748-6161173282774488583,bd31ced2-3002-4e06-934e-8eea54ab106a,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20SC-56B%20Build/RP1A.200720.012),,%0Aclick,2022-06-01%2000:29:10,2022-06-01%2000:30:03,2022-06-01%2000:30:03,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,23,Nakagawa,454-0842,392003,133.207.1.96,true,KDDI,KDDI,日本語,1654043398402-4149559073152929906,3cd1e92c-fd18-4c3e-b8c8-0ed0394039a9,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20KYV47%20Build/1.160HA.0217.a),https:/my.paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-05-31%2015:45:06,2022-06-01%2000:29:33,2022-06-01%2000:29:33,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V1_Install_And,11563397199,And_USP,125859267720,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,10,Isesaki,372-0802,392001,153.170.206.128,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654043371832-6002468840108578960,d204ee6f-1a6e-4e89-b133-61493fb6ec08,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20Pixel%203%20XL%20Build/SP1A.210812.016.C1),,%0Aclick,2022-06-01%2000:26:22,2022-06-01%2000:26:46,2022-06-01%2000:26:46,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V1_Install_And,11563397199,And_USP,125859267720,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,40,Fukuoka,819-0164,392004,203.135.233.203,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654043202306-8838398831132158499,ade3d385-5fac-4378-9000-efaaa4a08427,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20F-52A%20Build/V43R048B),,%0Aclick,2022-05-25%2000:40:21,2022-06-01%2000:25:31,2022-06-01%2000:25:31,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,27,Neyagawa,572-0850,392002,126.255.108.221,false,SoftBank,SoftBank,日本語,1654043130538-5678517006154546734,df4aa032-4bc3-42cf-8921-edd97d2f2efa,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20901SO%20Build/55.1.B.0.622),https:/my.paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:24:20,2022-06-01%2000:24:46,2022-06-01%2000:24:46,install,,,,,SDK,,,website,smart_banner,,top,,B,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,38,Matsuyama,790-0038,392014,158.201.248.179,true,KDDI,KDDI,日本語,1654043084241-8445274730636192394,98c986cf-c52f-4efb-aa30-f16d63a693bb,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20OPG03%20Build/SKQ1.210216.001),https:/paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=4fb4ce4f-af53-485c-af67-16b315ca5c6c&af_banner=true&af_channel=smart_banner&pid=website&c=top&af_adset=B&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:24:08,2022-06-01%2000:24:37,2022-06-01%2000:24:37,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,13,Suginami,166-0004,392001,60.108.139.225,true,Rakuten,Rakuten,日本語,1654043074387-1948889151306428782,dcf5aab5-50d4-4c4e-be01-f7ec08e8baa0,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20CPH2013%20Build/RKQ1.200903.002),,%0Aclick,2022-06-01%2000:21:45,2022-06-01%2000:22:58,2022-06-01%2000:22:58,install,,,,,SDK,,Adways,yjsearch_int,,ペイディ,13663900480,13663900480,127683670441,127683670441,582193433023,582193433023,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,13,Heiwajima,143-0006,392001,49.106.201.222,false,SoftBank,,日本語,1654042968586-7136318663824666300,f02d0eb9-b12a-4980-b11a-5f82aa7cd62e,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20801FJ%20Build/V46R054B),,%0Aclick,2022-06-01%2000:21:27,2022-06-01%2000:22:32,2022-06-01%2000:22:32,install,,,,,SDK,,Adways,bytedanceglobal_int,Pangle,,Adways__AF_【Pangle】Paidy_And___001,1725078960644113,【Pangle】And_動画_アドレス_LC,1725089198493714,000000_0006sec_220126203850.mp4,1725341243235361,5,900000000,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,27,Suita,564-0038,392002,92.202.12.140,true,Rakuten,Rakuten,日本語,1654042948131-6854664939529859973,09e013a2-d011-42be-9eb6-88a6d0594b80,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20CPH2099%20Build/QKQ1.200614.002),,https:/app.appsflyer.com/com.paidy.paidy%3Fpid=bytedanceglobal_int&af_siteid=900000000&c=Adways__AF_%25E3%2580%2590Pangle%25E3%2580%2591Paidy_And___001&af_channel=Pangle&af_c_id=1725078960644113&af_adset=%25E3%2580%2590Pangle%25E3%2580%2591And_%25E5%258B%2595%25E7%2594%25BB_%25E3%2582%25A2%25E3%2583%2589%25E3%2583%25AC%25E3%2582%25B9_LC&af_adset_id=1725089198493714&af_ad=000000_0006sec_220126203850.mp4&af_ad_id=1725341243235361&af_ad_type=5&af_click_lookback=7d&clickid=E.C.P.CqMBb9EX4Y5Npane9GyPL_7a4TuaXSecZFmm7_d4S4YPeeRXKhZsus3JYqvCCbQEsuC0LadNguBTQo6QG3mgW5UWsTMRKMknyPLQ9BlLTjw4_P6LRQh63yf-jAFlAjk_oSzVxTiZWuTeeabCCNdAm0ZknhxS2djGjZM73-3lcXAWXc9Q6xJmasSNOCyMqeqr-Y0ojTSs7EeQedwLJR_NidYUZ6ahAxIEdjIuMBogSG5YU7_7b8R4bR-9SM2n9OWsHkVNy3o8gH8BT3tpYrY&advertising_id=09e013a2-d011-42be-9eb6-88a6d0594b80&idfa=&os=0&af_ip=92.202.12.140&af_ua=Mozilla%252F5.0+%2528Linux%253B+Android+10%253B+CPH2099+Build%252FQKQ1.200614.002%253B+wv%2529+AppleWebKit%252F537.36+%2528KHTML%252C+like+Gecko%2529+Version%252F4.0+Chrome%252F101.0.4951.61+Mobile+Safari%252F537.36&af_lang=ja&redirect=false&af_prt=Adways&toutiaohaiwai=1%0Aclick,2022-05-31%2023:40:42,2022-06-01%2000:21:43,2022-06-01%2000:21:43,install,,,,,SDK,,Adways,googleadwords_int,ACI_Youtube,,Adways__x0000K4L_V3_forVideo_And,16103408041,And_Forvideo_V3,142523503864,,,ClickToDownload,YouTubeVideos,,,,,,engaged_view,,,,,,,,,,,,,,,,,,,AS,JP,28,Amagasaki,661-0012,392002,106.146.93.228,false,KDDI,KDDI,日本語,1654042900075-7577578300469515230,32c1f364-4c59-46b7-920a-5d64a030d586,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20SCG01%20Build/QP1A.190711.020),,%0Aclick,2022-05-27%022:39:14,2022-06-01%2000:20:39,2022-06-01%2000:20:39,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,adways,twitter,Adways__定常_Paidy_And_CPC_BR_新規_TW_JP___001,click,2022-05-26%2002:48:47,,,,,,,,,,,AS,JP,22,Atami,413-0012,392005,49.97.23.73,false,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654042836397-125418082250721726,7ba1d398-1998-46bf-96df-ea6812ccb5f0,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20SO-52A%20Build/58.1.B.6.17),,%0Aclick,2022-06-01%2000:11:40,2022-06-01%2000:12:09,2022-06-01%2000:12:09,install,,,,,SDK,,,website,smart_banner,,top,,B,,,,,,,,,,,,,,,Adways,googleadwords_int,Adways__x0000K4L_V3_forVideo_And,click,2022-05-31%2022:22:23,,,,,,,,,,,AS,JP,28,Itami,664-0847,392002,126.167.87.178,true,Y!mobile,Y!mobile,日本語,1654042326227-1303070282909645795,3b90eb14-b917-41ae-b8e6-62fd5b0d860d,,,,,,android,,11,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2011;%20A002OP%20Build/RKQ1.200903.002),https:/paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=4fb4ce4f-af53-485c-af67-16b315ca5c6c&af_banner=true&af_channel=smart_banner&pid=website&c=top&af_adset=B&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:11:26,2022-06-01%2000:12:00,2022-06-01%2000:12:00,install,,,,,SDK,,,website,,,QRcode_payment_page,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,06,Sakata,998-0858,None,14.9.103.192,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654042318668-4859753829449027203,35401fb0-a776-434b-bac5-3dad278df83b,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20SC-02K%20Build/QP1A.190711.020),,https:/paidy.onelink.me/GTiS/42946364%3Faf_qr=true%0Aclick,2022-05-31%2016:31:59,2022-06-01%2000:11:56,2022-06-01%2000:11:56,install,,,,,SDK,,Adways,googleadwords_int,ACI_Youtube,,Adways__x0000K4L_V3_forVideo_And,16103408041,And_Forvideo_V3,142523503864,,,ClickToDownload,YouTubeVideos,,,,,,engaged_view,,,,,,,,,,,,,,,,,,,AS,JP,27,Matsubara,580-0031,392002,49.250.61.57,true,au,KDDI,日本語,1654042313864-3840120515533817210,2415a0ec-f36d-4c67-8e16-fd76c61dc675,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20SOV42%20Build/56.1.C.3.231),,%0Aclick,2022-06-01%2000:04:09,2022-06-01%2000:05:21,2022-06-01%2000:05:21,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,44,Oita,870-0918,None,106.128.186.219,false,KDDI,KDDI,日本語,1654041914531-7111083311672304036,879282d2-f088-43df-b799-83987506d0c8,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20SCV43%20Build/QP1A.190711.020),https:/my.paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:03:08,2022-06-01%2000:03:53,2022-06-01%2000:03:53,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,14,Odawara,250-0865,392001,60.134.236.221,true,NTT%20DOCOMO,NTT%20DOCOMO,日本語,1654041830958-6481830250013709480,b7267aee-8ab3-410b-84a6-9aa0cc013fa7,,,,,,android,,9,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%209;%20SH-M08%20Build/SB070),,%0Aclick,2022-06-01%2000:01:39,2022-06-01%2000:03:49,2022-06-01%2000:03:49,install,,,,,SDK,,Adways,googleadwords_int,ACI_Youtube,,Adways__x0000K4L_V3_forVideo_And,16103408041,And_Forvideo_V3,142523503864,,,ClickToDownload,YouTubeVideos,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,31,Yonago,683-0824,None,126.213.33.118,true,SoftBank,SoftBank,日本語,1654041827295-8286395988615704513,00d71f4d-97ed-4bdc-8833-a973e5615f42,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20A001XM%20MIUI/V12.0.15.0.QJEJPSB),,%0Aclick,2022-06-01%2000:02:09,2022-06-01%2000:02:58,2022-06-01%2000:02:58,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,14,Yokohama,241-0831,392001,126.208.209.217,false,Y!mobile,Y!mobile,日本語,1654041775539-3649401917331189476,0da958ba-69c6-475d-8fb6-40db18862593,,,,,,android,,9,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%209;%20901ZT%20Build/PKQ1.190616.001),https:/my.paidy.com/,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-06-01%2000:01:58,2022-06-01%2000:02:13,2022-06-01%2000:02:13,install,,,,,SDK,,,MyPaidy,smart_banner,,top,,1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,23,Tokai,476-0003,392003,126.157.101.203,true,SoftBank,SoftBank,日本語,1654041733862-1361839909922007003,e4493aab-d423-44ff-aa21-e067c6174318,,,,,,android,,12,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2012;%20Pixel%206%20Build/SP2A.220505.002),https:/my.paidy.com/home,https:/paidy.onelink.me/GTiS%3Fcreative_id=5de10108-af0f-424c-a3e4-29951cc33518&af_banner=true&af_channel=smart_banner&pid=MyPaidy&c=top&af_adset=1&af_banner_build=static&af_banner_config=static&af_banner_sdk_ver=2&af_token=&%0Aclick,2022-05-31%2023:44:39,2022-06-01%2000:00:47,2022-06-01%2000:00:47,install,,,,,SDK,,Adways,googleadwords_int,ACI_Search,,Adways__x0000K4L_V3_login_And,16171942474,And_定常_V3,136223242027,,,ClickToDownload,GoogleSearch,,,,,,,,,,,,,,,,,,,,,,,,,AS,JP,12,Matsudo,271-0051,392001,126.179.123.44,false,Y!mobile,Y!mobile,日本語,1654041641692-7232239878329512953,f3776621-a35f-4a72-a371-827a6fbd5d90,,,,,,android,,10,2.10.0,v6.2.0,com.paidy.paidy,ペイディ%20(Paidy)%20-%20あと払いできる決済サービス,com.paidy.paidy,false,,7d,,,Dalvik/2.1.0%20(Linux;%20U;%20Android%2010;%20S3-SH%20Build/S2021),,%0A"""


    val reportCols = responseLines(0).toString.split(",").slice(0, 24)

    val appsflyerSchema =
      StructType(reportCols.map(x => StructField(x.replace(" ", "").toLowerCase,StringType, nullable = true)))

    val csvBody = responseLines(1).toString.split("%0A") //.replace("%20", " ")
    val csvRDD = spark.sparkContext.parallelize(csvBody).map(x => x.split(",").slice(0, 24))

    spark.sqlContext.createDataFrame(csvRDD.map(x => Row.fromSeq(x)), appsflyerSchema).show
*/


  }
}
