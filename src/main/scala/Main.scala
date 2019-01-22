import java.text.SimpleDateFormat
import java.util.Calendar

import de.uni_mannheim.minie.MinIE
import de.uni_mannheim.minie.annotation.AnnotatedProposition
import de.uni_mannheim.utils.coreNLP.CoreNLPUtils
import de.uni_mannheim.utils.Dictionary
import de.uni_mannheim.minie.annotation.Polarity
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Row, SparkSession, types}
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.semgraph.SemanticGraph
import it.unimi.dsi.fastutil.objects.ObjectArrayList

object Main {

  def main(args: Array[String]): Unit = {
    // Check arguments
    if (args.length < 3) {
      System.err.println("Usage: Main <data_path> <out_path> <n_partitions>")
      System.exit(1)
    }

    // get args
    val data_path = args(0)
    val out_dir = args(1)
    val n_partitions = args(2).toInt
    //    val n_workers = args(3).toInt
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss")
    val timestamp = formatter.format(now)
    val out_path = s"$out_dir/$timestamp"

    val spark = SparkSession
      .builder()
      .master(s"local[*]")
      .appName("MinIE-Spark Processor")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // 처리 시작
    val df = spark.read.parquet(data_path).toDF()
    val rows = df.filter(row => !row(1).toString.trim.isEmpty)

    val totalCount = sc.broadcast(df.count())
    println("Total count: " + totalCount.value)

    // Run
    val results = rows.repartition(n_partitions).mapPartitions(row => {
      // Initialize the parser and MinIE// Initialize the parser and MinIE
      val parser: StanfordCoreNLP = CoreNLPUtils.StanfordDepNNParser()
      var sg: SemanticGraph = new SemanticGraph()
      //      val dictionaries = Array[String]("/minie-resources/wiki-freq-rels-mw.txt", "/minie-resources/wiki-freq-args-mw.txt")
      //      val dict = new Dictionary(dictionaries)
      val minie: MinIE = new MinIE()

      // Extract
      val results_ = row.flatMap(r => {
        val id: String = r(0).toString
        val sentence: String = r(1).toString
        val sid: String = r(2).toString

        // process data
        try {
          sg = CoreNLPUtils.parse(parser, sentence)
          minie.minimize(sentence, sg, MinIE.Mode.SAFE, null)
          //          minie.minimize(sentence, sg, MinIE.Mode.DICTIONARY, dict)
        } catch {
          case e: Exception =>
            (id, sentence, sid, "")
        }

        // Do stuff with the triples// Do stuff with the triples
        val props: ObjectArrayList[AnnotatedProposition] = minie.getPropositions.clone()

        // Clear the object for re-usability
        minie.clear

        // return
        props.elements().map(prop => {
          // triple
          val subj = Option(prop.getSubject).getOrElse("")
          val rel = Option(prop.getRelation).getOrElse("")
          val obj = Option(prop.getObject).getOrElse("")

          // annotations
          val polarity = Option(prop.getPolarity.getType).getOrElse("")
          val modality = Option(prop.getModality.getModalityType).getOrElse("")
          val quantity = prop.getAllQuantities.elements().map(q => q.toString).mkString("|")

          val triple: String = s"$subj\t$rel\t$obj\t$polarity\t$modality"
          (id, sentence, sid, triple, quantity)
        })

      })

      // return
      results_
    })

    // Save
    results.toDF("id", "line", "sid", "triple", "quantity").write.option("compression", "snappy").parquet(out_path)
  }
}