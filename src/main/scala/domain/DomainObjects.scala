package domain

/**
  * Created by mirob on 8/19/2017.
  */
object DomainObjects {
  // For testing Json conversion
  final case class Item(name: String, id: Long)
  final case class Order(items: List[Item])
}
