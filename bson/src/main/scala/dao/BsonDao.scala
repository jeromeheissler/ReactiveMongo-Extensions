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

package reactivemongo.extensions.dao

import scala.util.Random
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._
import reactivemongo.bson._
import reactivemongo.extensions.dsl.functional.BsonDsl
import reactivemongo.api.{ bulk, DB, QueryOpts }
import reactivemongo.api.indexes.Index
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.core.commands.{ LastError, GetLastError, Count }
import play.api.libs.iteratee.{ Iteratee, Enumerator }
import org.joda.time.DateTime
import Handlers._

abstract class BsonDao[Model, ID](db: () => DB, collectionName: String)(implicit domainReader: BSONDocumentReader[Model],
  domainWriter: BSONDocumentWriter[Model],
  idWriter: BSONWriter[ID, _ <: BSONValue],
  lifeCycle: LifeCycle[Model, ID] = new ReflexiveLifeCycle[Model, ID])
    extends Dao[BSONCollection, BSONDocument, Model, ID, BSONDocumentWriter](db, collectionName)
    with BsonDsl {

  def ensureIndexes(): Future[Traversable[Boolean]] = Future sequence {
    autoIndexes map { index =>
      collection.indexesManager.ensure(index)
    }
  } map { results =>
    lifeCycle.ensuredIndexes()
    results
  }

  def listIndexes(): Future[List[Index]] = {
    collection.indexesManager.list()
  }

  def findOne(selector: BSONDocument = BSONDocument.empty): Future[Option[Model]] = {
    collection.find(selector).one[Model]
  }

  def findById(id: ID): Future[Option[Model]] = {
    findOne($id(id))
  }

  def findByIds(ids: Traversable[ID]): Future[List[Model]] = {
    findAll("_id" $in ids)
  }

  /**
   * @param page 1 based
   */
  def find(
    selector: BSONDocument = BSONDocument.empty,
    sort: BSONDocument = BSONDocument("_id" -> 1),
    page: Int,
    pageSize: Int): Future[List[Model]] = {
    val from = (page - 1) * pageSize
    collection
      .find(selector)
      .sort(sort)
      .options(QueryOpts(skipN = from, batchSizeN = pageSize))
      .cursor[Model]
      .collect[List](pageSize)
  }

  def findAll(
    selector: BSONDocument = BSONDocument.empty,
    sort: BSONDocument = BSONDocument("_id" -> 1)): Future[List[Model]] = {
    collection.find(selector).sort(sort).cursor[Model].collect[List]()
  }

  def findRandom(selector: BSONDocument = BSONDocument.empty): Future[Option[Model]] = {
    for {
      count <- count(selector)
      index = Random.nextInt(count)
      random <- collection.find(selector).options(QueryOpts(skipN = index, batchSizeN = 1)).one[Model]
    } yield random
  }

  def insert(model: Model, writeConcern: GetLastError = defaultWriteConcern): Future[LastError] = {
    val mappedModel = lifeCycle.prePersist(model)
    collection.insert(mappedModel, writeConcern) map { lastError =>
      lifeCycle.postPersist(mappedModel)
      lastError
    }
  }

  def bulkInsert(
    documents: TraversableOnce[Model],
    bulkSize: Int = bulk.MaxDocs,
    bulkByteSize: Int = bulk.MaxBulkSize): Future[Int] = {
    val mappedDocuments = documents.map(lifeCycle.prePersist)
    val enumerator = Enumerator.enumerate(mappedDocuments)
    collection.bulkInsert(enumerator, bulkSize, bulkByteSize) map { result =>
      mappedDocuments.map(lifeCycle.postPersist)
      result
    }
  }

  def update[U: BSONDocumentWriter](
    selector: BSONDocument,
    update: U,
    writeConcern: GetLastError = defaultWriteConcern,
    upsert: Boolean = false,
    multi: Boolean = false): Future[LastError] = {
    collection.update(selector, update, writeConcern, upsert, multi)
  }

  def updateById[U: BSONDocumentWriter](
    id: ID,
    update: U,
    writeConcern: GetLastError = defaultWriteConcern): Future[LastError] = {
    collection.update($id(id), update, writeConcern)
  }

  def save(model: Model, writeConcern: GetLastError = defaultWriteConcern): Future[LastError] = {
    val mappedModel = lifeCycle.prePersist(model)
    collection.save(mappedModel, writeConcern) map { lastError =>
      lifeCycle.postPersist(mappedModel)
      lastError
    }
  }

  def count(selector: BSONDocument = BSONDocument.empty): Future[Int] = {
    collection.db.command(Count(collectionName, Some(selector)))
  }

  def drop(): Future[Boolean] = {
    collection.drop()
  }

  def dropSync(timeout: Duration = 10 seconds): Boolean = {
    Await.result(drop(), timeout)
  }

  def removeById(id: ID, writeConcern: GetLastError = defaultWriteConcern): Future[LastError] = {
    lifeCycle.preRemove(id)
    collection.remove($id(id), writeConcern = defaultWriteConcern) map { lastError =>
      lifeCycle.postRemove(id)
      lastError
    }
  }

  def remove(
    query: BSONDocument,
    writeConcern: GetLastError = defaultWriteConcern,
    firstMatchOnly: Boolean = false): Future[LastError] = {
    collection.remove(query, writeConcern, firstMatchOnly)
  }

  def removeAll(writeConcern: GetLastError = defaultWriteConcern): Future[LastError] = {
    collection.remove(query = BSONDocument.empty, writeConcern = writeConcern, firstMatchOnly = false)
  }

  // Iteratee releated APIs

  /** Iteratee.foreach */
  def foreach(
    selector: BSONDocument = BSONDocument.empty,
    sort: BSONDocument = BSONDocument("_id" -> 1))(f: (Model) => Unit): Future[Unit] = {
    collection.find(selector).sort(sort).cursor[Model]
      .enumerate()
      .apply(Iteratee.foreach(f))
      .flatMap(i => i.run)
  }

  /** Iteratee.fold */
  def fold[A](
    selector: BSONDocument = BSONDocument.empty,
    sort: BSONDocument = BSONDocument("_id" -> 1),
    state: A)(f: (A, Model) => A): Future[A] = {
    collection.find(selector).sort(sort).cursor[Model]
      .enumerate()
      .apply(Iteratee.fold(state)(f))
      .flatMap(i => i.run)
  }

  ensureIndexes()
}
