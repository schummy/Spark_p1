/**
  * Created by Andrii_Krasnolob on 2/26/2016.
  */



class LogParserSparkTest extends SparkTestSpec{

  "A LogParser.Spark" should "return \"ip1,96955,48477.5\"" in {
    val input = sc.parallelize(Array("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",
    "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56927 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""))


   val result = LogParser.mapReduce(input)
   result.collect() should be (Array("ip1,96955,48477.5"))
  }

  it should "return \"ip1,96956,48478.0\"" in {
    val input = sc.parallelize(Array("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",
      "ip1 - - [24/Apr/2011:04:10:19 -0400] \"GET /~strabal/grease/photo1/97-13.jpg HTTP/1.1\" 200 56928 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""))


    val result = LogParser.mapReduce(input)
    result.collect() should be (Array("ip1,96956,48478.0"))
  }

  "A LogParser.toIntOrZero" should "convert \"-\" to 0 " in {
    val number = LogParser.toIntOrZero("-")
    number should be (0)
  }

  it should "convert corect String representation of Int to Int" in {
    val number = LogParser.toIntOrZero("1")
    number should be (1)
  }

  it should "convert String representation of Double to 0" in {
    val number = LogParser.toIntOrZero("1.0")
    number should be (0)
  }
}
