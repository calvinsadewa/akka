import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
import akka.routing.{Broadcast, RoundRobinRouter}
import scala.concurrent.duration._
import scala.util.Random

case class Datum(tipe:Int, content:String)

class LocalAggregator(reduceAgg: ActorRef) extends Actor {
  var jenisMap : Map[Int, List[Datum]] = Map()
  def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] =
    (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }
  def receive =
  {
    case data: List[Datum] =>
      jenisMap = mergeMap(List(jenisMap,data.groupBy(_.tipe)))((v1,v2) => v1 ++ v2);
    case complete: Boolean =>
      reduceAgg ! jenisMap
  }
}

/**
 * Global combiner to combine and print final output after aggregating results from local akka based combiners.
 */
class CountAggregator(threadCount: Int,system: ActorSystem) extends Actor {
  var globalMap : Map[Int, List[Datum]] = Map()
  var count: Integer = 0;

  def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] =
    (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }

  def receive =
  {
    case localCount: Map[Int, List[Datum]] =>
      //        count = count + 1
      globalMap = mergeMap(List(globalMap,localCount))((v1,v2) => v1 ++ v2)
      count = count + 1
      if (count == threadCount) {
        println("Got the completion message ... khallas!")
        onCompletion
      }
  }

  // print final word count on output
  def onCompletion() {

    for (jenis <- globalMap.keys) {
      print(jenis + "=>")
      println(globalMap.getOrElse(jenis,List()).size)
    }

    println("KeySize = "+globalMap.keys.size)
    print(s"Completed at ==>")
    println(System.currentTimeMillis())
    system.shutdown()
  }
}

object HelloAkkaScala extends App {

  val maxData = 10000000
  val disperancy = 1000
  val chunckSize = 1000
  val thread = 4
  // Create the 'helloakka' actor system
  val system = ActorSystem("helloakka")

  val global = system.actorOf(Props(new CountAggregator(thread,system)))
  val router = system.actorOf(Props(new LocalAggregator(global)).withRouter(RoundRobinRouter(nrOfInstances = thread)))

  val data = Range(0,maxData).map(i => Datum(Random.nextInt(disperancy),""+i)).toIterable
  println(data.size)
  print(s"Started at ==>")
  println(System.currentTimeMillis())

  println(data.grouped(chunckSize))
  data.grouped(chunckSize).foreach( l =>
    router ! l.toList//send chunks
  )

  router ! Broadcast (true) //broadcast end of day message!
}
