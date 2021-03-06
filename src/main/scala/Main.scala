package Main
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.mllib.linalg.{Vectors => AVec}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.Word2Vec
import scala.io.Source

import org.apache.spark.streaming._

import org.apache.log4j.Logger
import org.apache.log4j.Level

object main extends App {

  case class ModelResult(number: String, feedback: String, vect:Array[Double], cluster: Int)

  // Создание нового объекта конфигурации Spark
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkKMeans")
    .set("spark.executor.memory", "1048m")
    .set("spark.storageLevel", "MEMORY_ONLY")
    .set("spark.driver.memory", "512M")
    .set("spark.default.parallelism", "1")

  // Неявно подрубаем Контекст
  implicit val sc = new SparkContext(sparkConf)
  /*sc.setLogLevel("ERROR")*/
  // Создаём новую сессию
  val sparkSession = sql.SparkSession
    .builder
    .master("local")
    .appName("Spark Local")
    .getOrCreate()
  // По ходу написания кода возникли проблемы с преобразованием контейнеров в вектора спарка,
  // Пришлось добавить библиотеку с сериализаторами и дессериализаторами.
  import sparkSession.implicits._

  val ssc = new StreamingContext(sc, Seconds(2))
  // Достаём данные из CSV сандартной библиотекой source.io, не используем Spark DataFrame тут,
  // поскольку с ним возникает ряд проблем при попытке манипуляций с данными (ограничения в RDD)
  var datafile = Source.fromFile("/home/rjw/Data/neberitrubku_output.csv")
  val minedData: Seq[(String, Seq[String])] = datafile
    .getLines()
    .toSeq
    .tail
    .flatMap { line =>
      line.split("(\",\")|(\",)|(,\")") match {
        case cells if cells.length == 5 =>
          Some((cells(2), (cells(1) + " " + cells(4)).split("[\\s,;:\\.]").filter(_.length > 0).toSeq))
        case _ => None
      }
    }

  // После прведения всех манипуляций создаём датафрейм с двумя колонками - номерами телефона и списком слов
  // список формируется из полей title и description
  val dataset = sparkSession.createDataFrame(minedData).toDF("number", "words")

  // Получам из списка слов список векторов -- feature
  // Создаём екземпляр Word2Vec, говорим, что для построения векторов будем использовать колонку words,
  // результаты отправлять в колонку features
  // Размерность векторного пространства поставил 5 -- по большей части по опыту, меньше не стал,
  // поскольку датасет достаточно разнообразен,
  // с большим количеством измерений затруднительно работать при анализе
  val word2Vec = new Word2Vec()
    .setInputCol("words")
    .setOutputCol("features")
    .setVectorSize(5)
    .setMinCount(0)


  // прогоняем наш датасет через word2vec, получаем массив векторов (Array в Scala -- аналог Array в Java)
  val featuredDS = word2Vec
    .fit(dataset)
    .transform(dataset)
    .select("number", "features")

  // Смотрим, что у нас олучилось в результате -- показывает первые 20 результатов
  featuredDS.show()

  // Конвертируем наши вектора в Spark Dense Vector
  val parsedData = featuredDS.rdd.map { row =>
    val features: Array[Double] = row.toSeq.toArray.flatMap {
      case a: Double => Some(a)
      case _ => None
    }
    AVec.dense(features)
  }

  // Тренируем модель
  val clusters = KMeans.train(parsedData, 5, 20)

  val wssse = clusters.computeCost(parsedData)

  println(wssse)

  val predictedData = featuredDS.rdd.map { v =>
      ModelResult(v.get(0).toString, v.get(1).toString,v.getAs[Array[Double]]("vect"),
        clusters.predict(AVec.dense(v.getAs[Array[Double]]("vect"))))
    }
  //predictedData.toDF().createOrReplaceTempView("create_data")
  predictedData.toDF().coalesce(1)
    .write
    //.format("com.databricks.spark.json")
    .option("header", "true")
    .mode("append")
    .save("./mydata.json")

  //.write.csv("./data__.csv")
  // Результат сохраняем в файловую систему
  clusters.save(sc, path = "Models/KMeansTest2/neberitrubku")
  //ssc.start()
}