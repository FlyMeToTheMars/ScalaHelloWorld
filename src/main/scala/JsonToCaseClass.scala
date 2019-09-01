import scala.util.parsing.json.JSONObject

/**
  * @Author Administrator
  * @create 2019/8/22 9:22
  */
object JsonToCaseClass {
  def main(args: Array[String]): Unit = {

    val str = "{ \"firstName\":\"Bill\" , \"lastName\":\"Gates\" }"

    // 不使用FastJson 普通Json String转化object，使用JSON.parseFull
    import scala.util.parsing.json.JSON
    val ex1 = JSON.parseFull(str).get.asInstanceOf[Map[String,String]]("firstName")
    val ex2 = JSON.parseFull(str).get.asInstanceOf[Map[String,String]]("lastName")
    println(ex1)
    println(ex2)

    // Json与Map互相转化
    val colors:Map[String,String] = Map("red" -> "123456","azure" -> "789789")
    println(colors)
    val json = JSONObject(colors)



  }
}
