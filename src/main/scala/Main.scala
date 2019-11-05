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
    // val n_workers = args(3).toInt
    // val now = Calendar.getInstance().getTime()
    // val formatter = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss")
    // val timestamp = formatter.format(now)
    // val out_path = s"$out_dir/$timestamp"
    val out_path = s"$out_dir"

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
    // println(df.columns.toSeq)
    // System.exit(1)

    // Run
    val results = rows
      .repartition(n_partitions)
      .mapPartitions(row => {
        // Initialize the parser and MinIE
        val parser: StanfordCoreNLP = CoreNLPUtils.StanfordDepNNParser()
        var sg: SemanticGraph = new SemanticGraph()
        val minie: MinIE = new MinIE()
        val dictionaries = Array[String](
          "/minie-resources/wiki-freq-rels-mw.txt",
          "/minie-resources/wiki-freq-args-mw.txt"
        )
        val dict = new Dictionary(dictionaries)

        // Extract
        val results_ = row.flatMap(r => {
          // (file, collection, p_id, s_id, sentence, index)
          // val id: String = r.lastOption
          val file: String = r(0).toString
          val collection: String = r(1).toString
          val p_id: String = r(2).toString
          val s_id: String = r(3).toString
          val sentence: String = r(4).toString

          // process data
          try {
            sg = CoreNLPUtils.parse(parser, sentence)
            // minie.minimize(sentence, sg, MinIE.Mode.SAFE, null)
            minie.minimize(sentence, sg, MinIE.Mode.DICTIONARY, dict)
          } catch {
            case e: Exception =>
              (sentence, s_id, "")
          }

          // Do stuff with the triples
          val props: ObjectArrayList[AnnotatedProposition] =
            minie.getPropositions.clone()

          // Clear the object for re-usability
          minie.clear

          // return
          props
            .elements()
            .map(prop => {
              // triple
              val subj: String = Option(prop.getSubject).getOrElse("").toString
              val rel: String = Option(prop.getRelation).getOrElse("").toString
              val obj: String = Option(prop.getObject).getOrElse("").toString

              // annotations
              val polarity: String =
                Option(prop.getPolarity.getType).getOrElse("").toString
              val modality: String =
                Option(prop.getModality.getModalityType).getOrElse("").toString
              val quantity: String = prop.getAllQuantities
                .elements()
                .filter(q => {
                  !Option(q).isEmpty
                })
                .map(q => q.toString)
                .mkString("|")

              // val triple: String =
              //   s"$subj\t$rel\t$obj\t$polarity\t$modality\t$quantity"
              (
                file,
                collection,
                p_id,
                s_id,
                sentence,
                subj,
                rel,
                obj,
                polarity,
                modality,
                quantity
              )
            })

        })

        // return
        results_
      })

    // Save
    results
      .toDF(
        "file",
        "collection",
        "p_id",
        "s_id",
        "sentence",
        "triple_subj",
        "triple_rel",
        "triple_obj",
        "triple_polarity",
        "triple_modality",
        "triple_quantity"
      )
      .write
      .option("compression", "gzip")
      .parquet(out_path)
    sc.stop()
  }
}
