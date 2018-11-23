import java.text.SimpleDateFormat
import java.util.Calendar

import de.uni_mannheim.minie.MinIE
import de.uni_mannheim.minie.annotation.AnnotatedProposition
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils
import de.uni_mannheim.utils.Dictionary
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{SparkSession, types}
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.semgraph.SemanticGraph
import it.unimi.dsi.fastutil.objects.ObjectArrayList

case class Record(file: String, sentence: String)

object Main {

  def main(args: Array[String]): Unit = {
    // Check arguments
    if (args.length < 2) {
      System.err.println("Usage: Main <data_path> <out_path>")
      System.exit(1)
    }

    // get args
    val data_path = args(0)
    val out_dir = args(1)
//    val n_partitions = args(2).toInt
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss")
    val timestamp = formatter.format(now)
    val out_path = s"$out_dir/$timestamp"

    val spark = SparkSession
      .builder()
      .appName("MinIE-Spark Processor")
      .getOrCreate()

    val sc = spark.sparkContext
    val schema = types.StructType(
      StructField("file", StringType, true) ::
        StructField("sentence", StringType, true) :: Nil
    )
    val df = spark.read.schema(schema).parquet(data_path)
    //    df.show()

    // 처리 시작
    val totalCount = sc.broadcast(df.count())

    println("Total count: " + totalCount.value)

    import spark.implicits._
//    implicit val RecordEncoder = org.apache.spark.sql.Encoders.kryo[Record]

    // get (file, sentence) pairs
    val rows = df.as[Record].filter(row => (!row.file.trim.isEmpty && !row.sentence.trim.isEmpty))

    // Run
    val results = rows.mapPartitions(row => {
      // Initialize the parser and MinIE// Initialize the parser and MinIE
      val parser: StanfordCoreNLP = CoreNLPUtils.StanfordDepNNParser
      var sg: SemanticGraph = new SemanticGraph()
      val filenames = Array[String]("/minie-resources/nyt-freq-rels-mw.txt", "/minie-resources/nyt-freq-args-mw.txt")
      val dict = new Dictionary(filenames)
      val minie: MinIE = new MinIE()

      // Extract
      val results = row.map(file_and_sent => {
        val file = file_and_sent.file
        val sentence = file_and_sent.sentence

        // process data// process data
        sg = CoreNLPUtils.parse(parser, sentence)
//        minie.minimize(sentence, sg, MinIE.Mode.SAFE, null)
        minie.minimize(sentence, sg, MinIE.Mode.DICTIONARY, dict)

        // Do stuff with the triples// Do stuff with the triples
        val props: ObjectArrayList[AnnotatedProposition] = minie.getPropositions

        // Clear the object for re-usability
        minie.clear

        // return
        props.elements().map(prop => {
          val subj: String = prop.getSubject.toString
          val rel: String = prop.getRelation.toString
          val obj: String = prop.getObject.toString
          val result: String = String.format("%s\t%s\t%s\t%s", sentence, subj, rel, obj)
          (file, sentence, result)
        })
      })

      // return
      results
    })

    // Save
    val df_results = results.toDF("file", "sentence", "result")
    df_results.write.option("compression", "snappy").parquet(out_path)
  }
}