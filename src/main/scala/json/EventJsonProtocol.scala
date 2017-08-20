package json

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import domain.DomainObjects.{Item, Order}
import spray.json.DefaultJsonProtocol

// collect your json format instances into a support trait:
trait EventJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val itemFormat = jsonFormat2(Item)
  implicit val orderFormat = jsonFormat1(Order) // contains List[Item]
}