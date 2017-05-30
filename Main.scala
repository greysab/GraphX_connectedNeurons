import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.TriangleCountX
import org.apache.spark.graphx.lib
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.storage.StorageLevel
object Main{

def main(args: Array[String]) {


val conf = new SparkConf().setAppName("Count")
val sc = new SparkContext(conf)

println("Job starting")
val t1: Long = System.currentTimeMillis

val graph = GraphLoader.edgeListFile(sc, args(0), numEdgePartitions=args(1).toInt, edgeStorageLevel = StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY).partitionBy(PartitionStrategy.RandomVertexCut)


graph.cache()

var node1 = graph.pickRandomVertex()
var node2 = graph.pickRandomVertex()
while(node1==node2){
	node2=graph.pickRandomVertex()
}
println("node1: "+node1)
println("node2: "+node2)
if(graph.edges.filter(v=> v.srcId==node1 && v.dstId==node2).isEmpty())
{
	println("Edge does not exists, creating it..")
	val defVal=1
	val graphV = Graph(
	    graph.vertices,
	    graph.edges.union(sc.parallelize(Seq(Edge(node1, node2,1))))).partitionBy(PartitionStrategy.RandomVertexCut)	
	graphV.persist( StorageLevel.MEMORY_ONLY).numVertices
	val t0: Long = System.currentTimeMillis
	val count:EdgeRDD[Int]=TriangleCountX.run(graphV)
	println("Number of triangles: ")
	count.filter( v => v.srcId==node1 && v.dstId==node2).collect.foreach(v => println(v.attr))

	val t2: Long = System.currentTimeMillis
	println("Job finished in "+((t2-t1)/1000))
	println("Triangle counting finished in "+((t2-t0)/ 1000))


}
else{
	println("edge exists! ")
        val t0: Long = System.currentTimeMillis
	val count:EdgeRDD[Int]=TriangleCountX.run(graph)
	println("Number of triangles: ")
	count.filter( v =>  v.srcId==node1 && v.dstId==node2).collect.foreach(v => println(v.attr))

	val t2: Long = System.currentTimeMillis
	println("Job finished in "+((t2-t1)/ 1000))
	println("Triangle counting finished in "+((t2-t0)/ 1000))

}


}
}
