import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg.DenseVector
import breeze.plot.{plot, _}
import org.apache.spark
object TrafficData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TrafficAnalyze_1.3")
    /**
     * 重要点位交通量分析
     * 600019005000
     * 西陵-沿江大道市政府卡口东向西（市政府门口）
     * 600312003000
     * 高新-城东大道大连路口（云集隧道出口）
     * 202550006100
     * 白洋-S225省道枝城大桥卡口北向南
     * 202550000000
     * 白洋-S225省道枝城大桥卡口南向北
     */
    val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    val data = sc.textFile("E:\\1911\\数据")
    val fields: RDD[Array[String]] = data.map(_.trim
      .split("\u0001")).filter(_(46)=="2019")
    /**
     * 600019005000
     * 西陵-沿江大道市政府卡口东向西（市政府门口）
     * 道路编码，方向，年，月，日，时
     */
    val f1=Figure()
    val p1 = f1.subplot(0)
    p1.xlabel = "小时"
    p1.ylabel = "车流量"
    p1.title = ("市政府11月小时交通量分析")
    val ReFilter: RDD[Array[String]] = fields.filter(_(4)=="600019005000").filter(_(47)=="11")
    /**
     * 由东向西01
     */
//    val ReFilterEtoW: RDD[Array[String]] = ReFilter.filter(_(3)=="01")
//    val data_government_01 = ReFilterEtoW.map(f=>(f(49),f(47))).countByKey()
//    val data_EtoW= data_government_01.map(x=>(x._1,x._2.toString)).toArray.sortBy(_._1)
//    val dayEtoW:Array[Float] = data_EtoW.map(x=>(x._1.toFloat))
//    val flowEtoW:Array[Float] = data_EtoW.map(x=>(x._2.toFloat))
//    p1 += plot( DenseVector(dayEtoW), DenseVector(flowEtoW),style='-',colorcode="r",name="市政府11月小时交通量分析")
//    //p1 += plot( DenseVector(dayEtoW), DenseVector(flowEtoW),style='+',colorcode="b")
    /**
     * 由西向东02
     */
//    val ReFilterWtoE: RDD[Array[String]] = ReFilter.filter(_(3)=="02")
//    val data_government_02 = ReFilterWtoE.map(f=>(f(48),f(47))).countByKey()
//    val data_WtoE= data_government_02.map(x=>(x._1,x._2.toString)).toArray.sortBy(_._1)
//    val dayWtoE:Array[Float] = data_WtoE.map(x=>(x._1.toFloat))
//    val flowWtoE:Array[Float] = data_WtoE.map(x=>(x._2.toFloat))
//    p1 += plot( DenseVector(dayWtoE), DenseVector(flowWtoE),style='-',colorcode="c",name="云集隧道由西向东方向交通量")
    /**
     * 由南向北03
     */
//    val ReFilterStoN: RDD[Array[String]] = ReFilter.filter(_(3)=="03")
//    val data_government_03 = ReFilterStoN.map(f=>(f(48),f(47))).countByKey()
//    val data_StoN= data_government_03.map(x=>(x._1,x._2.toString)).toArray.sortBy(_._1)
//    val dayStoN:Array[Float] = data_StoN.map(x=>(x._1.toFloat))
//    val flowStoN:Array[Float] = data_StoN.map(x=>(x._2.toFloat))
//    p1 += plot( DenseVector(dayStoN), DenseVector(flowStoN),style='-',colorcode="b",name="云集隧道由南向北方向交通量")
    /**
     * 由北向南04
     */
//    val ReFilterNtoS: RDD[Array[String]] = ReFilter.filter(_(3)=="04")
//    val data_government_04 = ReFilterNtoS.map(f=>(f(48),f(47))).countByKey()
//    val data_NtoS= data_government_04.map(x=>(x._1,x._2.toString)).toArray.sortBy(_._1)
//    val dayNtoS:Array[Float] = data_NtoS.map(x=>(x._1.toFloat))
//    val flowNtoS:Array[Float] = data_NtoS.map(x=>(x._2.toFloat))
//    p1 += plot( DenseVector(dayNtoS), DenseVector(flowNtoS),style='-',colorcode="g",name="云集隧道由北向南方向交通量")
    /**
     * 其他方向99
     */
    val ReFilterEtoW: RDD[Array[String]] = ReFilter.filter(_(3)=="01")
    val data_government_01 = ReFilterEtoW.map(f=>(f(16),f(47))).countByKey()
    val data_EtoW: Array[(String, String)] = data_government_01.map(x=>(x._1,x._2.toString)).toArray.sortBy(_._1)
    data_EtoW.foreach(println)
    //val file = spark.SparkContext.parallelize(data_EtoW)
    //f1.saveas("C:\\Users\\yangjuncai\\Desktop\\大学学习\\大三小学期\\大数据基础实训\\云集隧道\\flow_YunJiTunnel_80GWtoE.png")
  }
}
