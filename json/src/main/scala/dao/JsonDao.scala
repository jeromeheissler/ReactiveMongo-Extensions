// Copyright (C) 2014 Fehmi Can Saglam (@fehmicans) and contributors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reactivemongo.extensions.json.dao

import scala.util.Random

import scala.concurrent.{ Future, Await, ExecutionContext }

import scala.concurrent.duration._

import reactivemongo.api.{ CursorFlattener, ReadPreference, DB, QueryOpts }
import reactivemongo.api.indexes.Index
import reactivemongo.api.commands.{ GetLastError, WriteResult }
import reactivemongo.extensions.dao.{ Dao, LifeCycle, ReflexiveLifeCycle }
import reactivemongo.extensions.json.dsl.JsonDsl._

import reactivemongo.play.iteratees.cursorProducer
import reactivemongo.play.json._, collection.JSONCollection

import play.api.libs.json.{
  Json,
  JsError,
  JsSuccess,
  JsObject,
  OFormat,
  OWrites,
  Reads,
  Writes
}
import play.api.libs.iteratee.Iteratee

/**
 * A DAO implementation that operates on JSONCollection using JsObject.
 *
 * To create a DAO for a concrete model extend this class.
 *
 * Below is a sample model.
 * {{{
 * import reactivemongo.bson.BSONObjectID
 * import play.api.libs.json.Json
 * import reactivemongo.play.json.BSONFormats._
 *
 * case class Person(
 *   _id: BSONObjectID = BSONObjectID.generate,
 *   name: String,
 *   surname: String,
 *   age: Int)
 *
 * object Person {
 *   implicit val personFormat = Json.format[Person]
 * }
 *
 * }}}
 *
 * To define a JsonDao for the Person model you just need to extend JsonDao.
 *
 * {{{
 * import reactivemongo.api.{ MongoDriver, DB }
 * import reactivemongo.bson.BSONObjectID
 * import reactivemongo.play.json.BSONFormats._
 * import reactivemongo.extensions.json.dao.JsonDao
 * import scala.concurrent.ExecutionContext.Implicits.global
 *
 *
 * object MongoContext {
 *  val driver = new MongoDriver
 *  val connection = driver.connection(List("localhost"))
 *  def db(): DB = connection("reactivemongo-extensions")
 * }
 *
 * object PersonDao extends JsonDao[Person, BSONObjectID](MongoContext.db, "persons")
 * }}}
 *
 * @param db A parameterless function returning a [[reactivemongo.api.DB]] instance.
 * @param collectionName Name of the collection this DAO is going to operate on.
 * @param lifeCycle [[reactivemongo.extensions.dao.LifeCycle]] for the Model type.
 * @tparam Model Type of the model that this DAO uses.
 * @tparam ID Type of the ID field of the model.
 */
abstract class JsonDao[Model: OFormat, ID: Writes](db: => Future[DB], collectionName: String)(implicit lifeCycle: LifeCycle[Model, ID] = new ReflexiveLifeCycle[Model, ID], ec: ExecutionContext)
    extends Dao[JSONCollection, JsObject, Model, ID, OWrites](
      db, collectionName) {

  def ensureIndexes()(implicit ec: ExecutionContext): Future[Traversable[Boolean]] = Future sequence {
    autoIndexes map { index =>
      collection.flatMap(_.indexesManager.ensure(index))
    }
  }.map { results =>
    lifeCycle.ensuredIndexes()
    results
  }

  def listIndexes()(implicit ec: ExecutionContext): Future[List[Index]] =
    collection.flatMap(_.indexesManager.list())

  def findOne(selector: JsObject = Json.obj())(implicit ec: ExecutionContext): Future[Option[Model]] = collection.flatMap(_.find(selector).one[Model])

  def findById(id: ID)(implicit ec: ExecutionContext): Future[Option[Model]] =
    findOne($id(id))

  def findByIds(ids: ID*)(implicit ec: ExecutionContext): Future[List[Model]] =
    findAll("_id" $in (ids: _*))

  def find(
    selector: JsObject = Json.obj(),
    sort: JsObject = Json.obj("_id" -> 1),
    page: Int,
    pageSize: Int,
    readPreference: ReadPreference = ReadPreference.primary)(implicit ec: ExecutionContext): Future[List[Model]] = {
    val from = (page - 1) * pageSize
    collection.flatMap(_
      .find(selector)
      .sort(sort)
      .options(QueryOpts(skipN = from, batchSizeN = pageSize))
      .cursor[Model](readPreference)
      .collect[List](pageSize)
    )
  }

  def findAll(
    selector: JsObject = Json.obj(),
    sort: JsObject = Json.obj("_id" -> 1),
    readPreference: ReadPreference = ReadPreference.primary)(implicit ec: ExecutionContext): Future[List[Model]] = {
    collection.flatMap(_.find(selector).sort(sort).cursor[Model](readPreference).collect[List]())
  }

  def findRandom(selector: JsObject = Json.obj())(implicit ec: ExecutionContext): Future[Option[Model]] = for {
    count <- count(selector)
    index = Random.nextInt(count)
    random <- collection.flatMap(_.find(selector).options(QueryOpts(skipN = index, batchSizeN = 1)).one[Model])
  } yield random

  def insert(model: Model, writeConcern: GetLastError = defaultWriteConcern)(implicit ec: ExecutionContext): Future[WriteResult] = {
    val mappedModel = lifeCycle.prePersist(model)
    collection.flatMap(_.insert(mappedModel, writeConcern)) map { writeResult =>
      lifeCycle.postPersist(mappedModel)
      writeResult
    }
  }

  def bulkInsert(
    documents: TraversableOnce[Model])(implicit ec: ExecutionContext): Future[Int] = {
    val mappedDocuments = documents.map(lifeCycle.prePersist)
    val writer = implicitly[OWrites[Model]]

    def go(docs: Traversable[Model]): Stream[JsObject] = docs.headOption match {
      case Some(doc) => writer.writes(doc) #:: go(docs.tail)
      case _ => Stream.Empty
    }

    collection.flatMap(_.bulkInsert(go(mappedDocuments.toTraversable),
      ordered = true, defaultWriteConcern) map { result =>
        mappedDocuments.foreach(lifeCycle.postPersist)
        result.n
      })
  }

  def bulkInsert(
    documents: TraversableOnce[Model],
    bulkSize: Int,
    bulkByteSize: Int)(implicit ec: ExecutionContext): Future[Int] = {
    val mappedDocuments = documents.map(lifeCycle.prePersist)
    val writer = implicitly[OWrites[Model]]

    def go(docs: Traversable[Model]): Stream[JsObject] = docs.headOption match {
      case Some(doc) => writer.writes(doc) #:: go(docs.tail)
      case _ => Stream.Empty
    }

    collection.flatMap(_.bulkInsert(go(mappedDocuments.toTraversable),
      ordered = true, defaultWriteConcern, bulkSize, bulkByteSize) map { result =>
        mappedDocuments.foreach(lifeCycle.postPersist)
        result.n
      })
  }

  def update[U: OWrites](
    selector: JsObject,
    update: U,
    writeConcern: GetLastError = defaultWriteConcern,
    upsert: Boolean = false,
    multi: Boolean = false)(implicit ec: ExecutionContext): Future[WriteResult] = collection.flatMap(_.update(selector, update, writeConcern, upsert, multi))

  def updateById[U: OWrites](
    id: ID,
    update: U,
    writeConcern: GetLastError = defaultWriteConcern)(implicit ec: ExecutionContext): Future[WriteResult] = collection.flatMap(_.update($id(id), update, writeConcern))

  def count(selector: JsObject = Json.obj())(implicit ec: ExecutionContext): Future[Int] = collection.flatMap(_.count(Some(selector)))

  def dropSync(timeout: Duration = 10 seconds)(implicit ec: ExecutionContext): Unit = Await.result({
    collection.flatMap(_.drop(failIfNotFound = true))
  }, timeout)

  def removeById(id: ID, writeConcern: GetLastError = defaultWriteConcern)(implicit ec: ExecutionContext): Future[WriteResult] = {
    lifeCycle.preRemove(id)
    collection.flatMap(_.remove($id(id), writeConcern = defaultWriteConcern)) map { lastError =>
      lifeCycle.postRemove(id)
      lastError
    }
  }

  def remove(
    query: JsObject,
    writeConcern: GetLastError = defaultWriteConcern,
    firstMatchOnly: Boolean = false)(implicit ec: ExecutionContext): Future[WriteResult] = {
    collection.flatMap(_.remove(query, writeConcern, firstMatchOnly))
  }

  def removeAll(writeConcern: GetLastError = defaultWriteConcern)(implicit ec: ExecutionContext): Future[WriteResult] = {
    collection.flatMap(_.remove(query = Json.obj(), writeConcern = writeConcern, firstMatchOnly = false))
  }

  def foreach(
    selector: JsObject = Json.obj(),
    sort: JsObject = Json.obj("_id" -> 1),
    readPreference: ReadPreference = ReadPreference.primary)(f: (Model) => Unit)(implicit ec: ExecutionContext): Future[Unit] = {
    val cursor = CursorFlattener.defaultCursorFlattener.flatten(collection.map(_.find(selector).sort(sort).cursor[Model](readPreference)))
    cursorProducer.produce(cursor)
      .enumerator()
      .apply(Iteratee.foreach(f))
      .flatMap(i => i.run)
  }

  def fold[A](
    selector: JsObject = Json.obj(),
    sort: JsObject = Json.obj("_id" -> 1),
    state: A,
    readPreference: ReadPreference = ReadPreference.primary)(f: (A, Model) => A)(implicit ec: ExecutionContext): Future[A] = {
    val cursor = CursorFlattener.defaultCursorFlattener.flatten(collection.map(_.find(selector).sort(sort).cursor[Model](readPreference)))
    cursorProducer.produce(cursor)
      .enumerator()
      .apply(Iteratee.fold(state)(f))
      .flatMap(i => i.run)
  }

  ensureIndexes()
}

object JsonDao {
  def apply[Model: OFormat, ID: Writes](db: => Future[DB], collectionName: String)(
    implicit lifeCycle: LifeCycle[Model, ID] = new ReflexiveLifeCycle[Model, ID], ec: ExecutionContext): JsonDao[Model, ID] = new JsonDao[Model, ID](db, collectionName) {}

}
