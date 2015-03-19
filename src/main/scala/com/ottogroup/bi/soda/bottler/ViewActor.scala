package com.ottogroup.bi.soda.bottler

import java.lang.Math.max
import java.security.PrivilegedAction

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.ottogroup.bi.soda.SettingsImpl
import com.ottogroup.bi.soda.dsl.NoOp
import com.ottogroup.bi.soda.dsl.View
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Delete
import com.ottogroup.bi.soda.dsl.transformations.filesystem.FilesystemTransformation
import com.ottogroup.bi.soda.dsl.transformations.filesystem.Touch

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.Logging
import akka.event.LoggingReceive
import akka.pattern.ask
import akka.util.Timeout

class ViewActor(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef) extends Actor {
  import context._

  val log = Logging(system, this)
  //implicit val timeout = new Timeout(settings.dependencyTimout)

  val listenersWaitingForMaterialize = collection.mutable.HashSet[ActorRef]()
  val dependenciesMaterializing = collection.mutable.HashSet[View]()
  var oneDependencyReturnedData = false

  // state variables
  // timestamp of last transformation
  var lastTransformationTimestamp = 0l

  // one of the dependencies was not available (no data)
  var incomplete = false

  // maximum transformation timestamp of dependencies
  var dependenciesFreshness = 0l

  // one of the dependencies' transformations failed
  var withErrors = false

  override def postRestart(reason: Throwable) {
    self ! MaterializeView()
  }

  // State: default
  // transitions: defaultForViewWithoutDependencies, defaultForViewWithDependencies
  def receive = if (view.dependencies.isEmpty) {
    log.info(stateInfo("defaultForViewWithoutDependencies"))
    defaultForViewWithoutDependencies
  } else {
    log.info(stateInfo("defaultForViewWithDependencies"))
    defaultForViewWithDependencies
  }

  // State: default for non-NoOp views without dependencies
  // transitions: transforming
  def defaultForViewWithoutDependencies: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("receive", view)

    case MaterializeView() => {
      listenersWaitingForMaterialize.add(sender)
      toTransformOrMaterialize(0)
    }

    case NewDataAvailable(viewWithNewData) => if (view == viewWithNewData) toDefaultAndReload(false)
  })

  // State: default for views with dependencies
  // transitions: waiting
  def defaultForViewWithDependencies: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("receive", view)

    case MaterializeView() => toWaiting()

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData)) toDefaultAndReload()
  })

  // State: view actor waiting for dependencies to materialize
  // transitions: transforming, materialized, default
  def waiting: Receive = LoggingReceive {
    case _: GetStatus => sender ! ViewStatusResponse("waiting", view)

    case MaterializeView() => listenersWaitingForMaterialize.add(sender)

    case NoDataAvailable(dependency) => {
      log.debug("Nodata from " + dependency)
      incomplete = true
      dependencyAnswered(dependency)
    }

    case Failed(dependency) => {
      log.debug("Failed from " + dependency)
      incomplete = true
      withErrors = true
      dependencyAnswered(dependency)
    }

    case ViewMaterialized(dependency, dependencyIncomplete, dependencyTransformationTimestamp, dependencyWithErrors) => {
      log.debug(s"View materialized from ${dependency}: incomplete=${dependencyIncomplete} transformationTimestamp=${dependencyTransformationTimestamp} withErrors=${dependencyWithErrors}")
      oneDependencyReturnedData = true
      incomplete |= dependencyIncomplete
      withErrors |= dependencyWithErrors
      dependenciesFreshness = max(dependenciesFreshness, dependencyTransformationTimestamp)
      dependencyAnswered(dependency)
    }

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData)) self ! NewDataAvailable(viewWithNewData)
  }

  // State: transforming, view actor in process of applying transformation
  // transitions: materialized,retrying
  def transforming(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse(if (0.equals(retries)) "transforming" else "retrying", view)

    case _: ActionSuccess[_] => {
      log.info("SUCCESS")

      touchSuccessFlag(view)
      logTransformationTimestamp(view)

      toMaterialize()
    }

    case _: ActionFailure[_] => toRetrying(retries)

    case MaterializeView() => listenersWaitingForMaterialize.add(sender)

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData) || (view.dependencies.isEmpty && viewWithNewData == view)) self ! NewDataAvailable(viewWithNewData)
  })

  // State: retrying
  // transitions: failed, transforming
  def retrying(retries: Int): Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("retrying", view)

    case MaterializeView() => listenersWaitingForMaterialize.add(sender)

    case Retry() => if (retries <= settings.retries)
      toTransformOrMaterialize(retries + 1)
    else {

      log.warning(stateInfo("failed"))

      unbecome()
      become(failed)

      listenersWaitingForMaterialize.foreach(_ ! Failed(view))
      listenersWaitingForMaterialize.clear()
    }

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData) || (view.dependencies.isEmpty && viewWithNewData == view)) self ! NewDataAvailable(viewWithNewData)
  })

  // State: materialized, view has been computed and materialized
  // transitions: default,transforming
  def materialized: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("materialized", view)

    case MaterializeView() => sender ! ViewMaterialized(view, incomplete, getTransformationTimestamp(view), withErrors)

    case Invalidate() => toDefault()

    case NewDataAvailable(viewWithNewData) => if (view.dependencies.contains(viewWithNewData)) toDefaultAndReload()
    else if (view.dependencies.isEmpty && view == viewWithNewData) toDefaultAndReload(false)
  })

  // State: failed, view actor failed to materialize
  // transitions:  default, transforming
  def failed: Receive = LoggingReceive({
    case _: GetStatus => sender ! ViewStatusResponse("failed", view)

    case NewDataAvailable(viewWithNewData) =>
      if (view.dependencies.contains(viewWithNewData)) toDefaultAndReload()
      else if (view.dependencies.isEmpty && view == viewWithNewData) toDefaultAndReload(false)

    case Invalidate() => toDefault()

    case MaterializeView() => sender ! Failed(view)
  })

  def dependencyAnswered(dependency: com.ottogroup.bi.soda.dsl.View) {
    dependenciesMaterializing.remove(dependency)

    if (!dependenciesMaterializing.isEmpty) {
      log.debug(s"This actor is still waiting for ${dependenciesMaterializing.size} dependencies, dependencyFreshness=${dependenciesFreshness}, incomplete=${incomplete}, dependencies with data=${oneDependencyReturnedData}")
      return
    }

    if (oneDependencyReturnedData) {
      if ((getTransformationTimestamp(view) <= dependenciesFreshness) || hasVersionMismatch(view))
        toTransformOrMaterialize(0)
      else {
        toMaterialize()
      }
    } else {
      listenersWaitingForMaterialize.foreach(s => { log.debug(s"sending NoDataAvailable to ${s}"); s ! NoDataAvailable(view) })
      listenersWaitingForMaterialize.clear

      toDefault()
    }
  }

  def toDefault() {
    lastTransformationTimestamp = 0l
    dependenciesFreshness = 0l
    withErrors = false
    incomplete = false

    log.info(stateInfo("default"))

    unbecome()
    become(receive)
  }

  def toWaiting() {
    withErrors = false
    incomplete = false
    dependenciesFreshness = 0l

    listenersWaitingForMaterialize.add(sender)

    view.dependencies.foreach { d =>
      {
        dependenciesMaterializing.add(d)

        log.debug("materializing dependency " + d)

        val dependencyActor = Await.result(ask(viewManagerActor, d)(settings.viewManagerResponseTimeout), settings.viewManagerResponseTimeout).asInstanceOf[ActorRef]

        dependencyActor ! MaterializeView()
      }
    }

    log.info(stateInfo("waiting"))

    unbecome()
    become(waiting)
  }

  def toMaterialize() {
    log.info(stateInfo("materialized"))

    listenersWaitingForMaterialize.foreach(s => s ! ViewMaterialized(view, incomplete, getTransformationTimestamp(view), withErrors))
    listenersWaitingForMaterialize.clear

    unbecome()
    become(materialized)

    oneDependencyReturnedData = false
    dependenciesFreshness = 0l
  }

  def toTransformOrMaterialize(retries: Int) {
    view.transformation() match {
      case NoOp() => if (successFlagExists(view)) {
        log.debug("no dependencies for " + view + ", success flag exists, and no transformation specified")

        addPartition(view)
        setVersion(view)
        getOrLogTransformationTimestamp(view)

        toMaterialize()
      } else {
        log.debug("no data and no dependencies for " + view)

        listenersWaitingForMaterialize.foreach(s => s ! NoDataAvailable(view))
        listenersWaitingForMaterialize.clear

        toDefault()
      }

      case _: FilesystemTransformation => {
        addPartition(view)
        setVersion(view)

        if (getTransformationTimestamp(view) > 0l) {
          toMaterialize()
        } else {
          deletePartitionData(view)

          actionsManagerActor ! view

          log.info(stateInfo("transforming"))

          unbecome()
          become(transforming(retries))
        }
      }

      case _ => {
        addPartition(view)
        setVersion(view)
        deletePartitionData(view)

        actionsManagerActor ! view

        log.info(stateInfo("transforming"))

        unbecome()
        become(transforming(retries))
      }
    }

  }

  def toRetrying(retries: Int): akka.actor.Cancellable = {
    log.info(stateInfo("retrying"))

    unbecome()
    become(retrying(retries))

    // exponential backoff
    system.scheduler.scheduleOnce(Duration.create(Math.pow(2, retries).toLong, "seconds"))(self ! Retry())
  }

  def toDefaultAndReload(withPropagation: Boolean = true) {
    toDefault()

    self ! MaterializeView()
    if (withPropagation)
      viewManagerActor ! NewDataAvailable(view)
  }

  def successFlagExists(view: View): Boolean = {
    settings.userGroupInformation.doAs(new PrivilegedAction[Boolean]() {
      def run() = {
        val pathWithSuccessFlag = new Path(view.fullPath + "/_SUCCESS")

        FileSystem.get(settings.hadoopConf).exists(pathWithSuccessFlag)
      }
    })
  }

  def touchSuccessFlag(view: View) {
    Await.result(ask(actionsManagerActor, Touch(view.fullPath + "/_SUCCESS"))(settings.filesystemTimeout), settings.filesystemTimeout)
  }

  def hasVersionMismatch(view: View) = {
    val versionInfo = Await.result(ask(schemaActor, CheckViewVersion(view))(settings.schemaTimeout), settings.schemaTimeout)

    log.debug(versionInfo.toString)

    versionInfo match {
      case SchemaActionFailure() => {
        log.warning("got error from versioncheck, assuming version mismatch")
        true
      }

      case ViewVersionMismatch(v, version) => {
        log.debug("version mismatch")
        true
      }

      case ViewVersionOk(v) => false
    }
  }

  def getTransformationTimestamp(view: View) = {
    if (lastTransformationTimestamp == 0l) {
      lastTransformationTimestamp = Await.result(ask(schemaActor, GetTransformationTimestamp(view))(settings.schemaTimeout), settings.schemaTimeout) match {
        case TransformationTimestamp(_, ts) => ts
        case _ => 0l
      }
    }
    lastTransformationTimestamp
  }

  def logTransformationTimestamp(view: View) = {
    Await.result(ask(schemaActor, LogTransformationTimestamp(view))(settings.schemaTimeout), settings.schemaTimeout)
    getTransformationTimestamp(view)
  }

  def getOrLogTransformationTimestamp(view: View) = {
    val ts = getTransformationTimestamp(view)
    if (ts == 0l)
      logTransformationTimestamp(view)
    else ts
  }

  def addPartition(view: View) {
    Await.result(ask(schemaActor, AddPartition(view))(settings.schemaTimeout), settings.schemaTimeout)
  }

  def deletePartitionData(view: View) {
    Await.result(ask(actionsManagerActor, Delete(view.fullPath, true))(settings.filesystemTimeout), settings.filesystemTimeout)
  }

  def setVersion(view: View) {
    Await.result(ask(schemaActor, SetViewVersion(view))(settings.schemaTimeout), settings.schemaTimeout)
  }

  def stateInfo(stateName: String) =
    s"VIEWACTOR STATE CHANGE ===> ${stateName.toUpperCase()}: lastTransformationTimestamp=${lastTransformationTimestamp} dependenciesFreshness=${dependenciesFreshness} incomplete=${incomplete} withErrors=${withErrors}"
}

object ViewActor {
  def props(view: View, settings: SettingsImpl, viewManagerActor: ActorRef, actionsManagerActor: ActorRef, schemaActor: ActorRef): Props = Props(classOf[ViewActor], view, settings, viewManagerActor, actionsManagerActor, schemaActor)
}
