import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphX {
  def main(args: Array[String]) {

val file=sc.textFile("/brain-spark/nrn.csv")
val header = file.first() 
val data = file.filter(line => line != header)
val edges: RDD[Edge[String]] =
     data.map { line =>
        val fields = line.split(",")
        val f1=fields(0).toDouble
        val f2=fields(2).toDouble      
        Edge(f1.toLong, f2.toLong)
      }
//Build graph
val graph = Graph.fromEdges(edges, "defaultProperty")
//Build attributes
val meta= data.map(line => line.split(",")).map( parts => (parts(1).toLong, parts.slice(3,21)))
//Attach attributes to graph
val g = graph.outerJoinVertices(meta) {
  case (src, dst, Some(attrList)) => attrList}
}
 
//Compute connectedComponents
val cc = graph.connectedComponents().vertices
// Attache attributes to CC
val ccByMeta = meta.join(cc).map {
  case (id, (username, cc)) => (username(0), cc)
}
// Find how many CC the graph has
val s=cc.collect.toSet
val NumCom=s.groupBy(_._2).mapValues(_.size).toList


--------------------------------------


val graph = GraphLoader.edgeListFile(sc, file,true).partitionBy(PartitionStrategy.RandomVertexCut)
val triCounts = graph.triangleCount().vertices

val neib=graph.collectNeighborIds(EdgeDirection.Either)
val NG= neib.map{ case (vid,nb) => (vid,2/(nb.sum*(nb.sum-1)).toFloat)}
val tst=NG.zip(triCounts)
val id_coef=tst.map{case ((id1,k),(id2,t))=> (id1,k*t)}
val coeff=id_coef.map{case (v,c)=>  c}

s.saveAsTextFile("/nfs4/bbp.epfl.ch/scratch/test-spark/coef.txt")
