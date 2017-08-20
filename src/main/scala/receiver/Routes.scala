package receiver

import akka.http.scaladsl.server.Route

/**
  * Created by mirob on 8/19/2017.
  */
trait Routes extends DefaultRoute {
  val routes: Route = defaultRoute
}
