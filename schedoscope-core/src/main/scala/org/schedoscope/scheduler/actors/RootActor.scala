/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.scheduler.actors

import org.schedoscope.SchedoscopeSettings
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.AllForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy._
import akka.event.Logging

/**
 * Root actor of the Schedoscope scheduler actor system.
 * Merely a supervisor that shuts down Schedoscope in case anything gets escalated.
 */
class RootActor(settings: SchedoscopeSettings) extends Actor {
  import context._

  val log = Logging(system, this)

  var transformationManagerActor: ActorRef = null
  var schemaActor: ActorRef = null
  var viewManagerActor: ActorRef = null

  override val supervisorStrategy =
    AllForOneStrategy() {
      case t: Throwable => {
        t.printStackTrace()
        Escalate
      }
    }

  override def preStart {
    transformationManagerActor = actorOf(TransformationManagerActor.props(settings), "transformations")
    schemaActor = actorOf(SchemaActor.props(settings), "schema")
    viewManagerActor = actorOf(
      ViewManagerActor.props(settings,
        transformationManagerActor,
        schemaActor,
        schemaActor), "views")
  }

  def receive = {
    // we do not process any messages as we are merely a supervisor
    case _ => {}
  }
}

/**
 * Helpful constants to access the various actors in the schedoscope actor systems. These implicitly create
 * the actors upon first request.
 */
object RootActor {
  def props(settings: SchedoscopeSettings) = Props(classOf[RootActor], settings).withDispatcher("akka.actor.root-actor-dispatcher")
}