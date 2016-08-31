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

import java.io.OutputStream

import scala.concurrent.{ Await, Future, ExecutionContext }

import play.api.libs.iteratee.Enumerator

import reactivemongo.api._
import reactivemongo.api.gridfs.{
  DefaultFileToSave,
  ReadFile,
  FileToSave,
  GridFS,
  IdProducer,
  Implicits
}, Implicits.DefaultReadFileReader
import reactivemongo.bson.{ BSONDocumentReader, BSONDocumentWriter, BSONValue }
import reactivemongo.api.commands.WriteResult

import reactivemongo.extensions.dao.FileDao.ReadFileWrapper

/**
 * Base class for all File DAO implementations.
 *
 * @param db A [[reactivemongo.api.DB]] instance.
 * @param collectionName Name of the collection this DAO is going to operate on.
 */
abstract class FileDao[Id <% BSONValue: IdProducer, Structure](
    db: => Future[DB with DBMetaCommands], collectionName: String) {

  import FileDao.BSONReadFile

  /** Reference to the GridFS instance this FileDao operates on. */
  def gfs(implicit ec: ExecutionContext) = db.map(d => GridFS[BSONSerializationPack.type](d, collectionName))

  /**
   * Finds the files matching the given selector.
   *
   * @param selector Selector document
   * @return A cursor for the files matching the given selector.
   */
  def find(selector: Structure)(implicit sWriter: BSONDocumentWriter[Structure], ec: ExecutionContext): Cursor[BSONReadFile] = {
    CursorFlattener.defaultCursorFlattener.flatten(gfs.map(_.find[Structure, BSONReadFile](selector)))
  }

  /** Retrieves the file with the given `id`. */
  def findById(id: Id)(implicit ec: ExecutionContext): ReadFileWrapper

  /* Retrieves at most one file matching the given selector. */
  def findOne(selector: Structure)(implicit sWriter: BSONDocumentWriter[Structure], ec: ExecutionContext): ReadFileWrapper = ReadFileWrapper(gfs, gfs.flatMap(_.find(selector).headOption))

  /** Removes the file with the given `id`. */
  def removeById(id: Id)(implicit ec: ExecutionContext): Future[WriteResult] =
    gfs.flatMap(_.remove(implicitly[BSONValue](id)))

  /** Saves the content provided by the given enumerator with the given metadata. */
  def save(
    enumerator: Enumerator[Array[Byte]],
    file: FileToSave[BSONSerializationPack.type, BSONValue],
    chunkSize: Int = 262144)(implicit readFileReader: BSONDocumentReader[BSONReadFile], ec: ExecutionContext): Future[BSONReadFile] =
    gfs.flatMap(_.save(enumerator, file, chunkSize))

  /** Saves the content provided by the given enumerator with the given metadata. */
  def save(
    enumerator: Enumerator[Array[Byte]],
    filename: String,
    contentType: String)(implicit readFileReader: BSONDocumentReader[BSONReadFile], ec: ExecutionContext): Future[BSONReadFile] =
    gfs.flatMap(_.save(enumerator, DefaultFileToSave(
      filename = Some(filename), contentType = Some(contentType))))

}

object FileDao {
  type BSONReadFile = ReadFile[BSONSerializationPack.type, BSONValue]

  case class ReadFileWrapper(gridfs: Future[GridFS[BSONSerializationPack.type]], readFile: Future[Option[BSONReadFile]]) {

    @deprecated("Use [[gridfs]]", "0.11.14")
    def gfs = Await.result(gridfs, scala.concurrent.duration.Duration.Inf)

    def enumerate(implicit ec: ExecutionContext): Future[Option[Enumerator[Array[Byte]]]] = gridfs.flatMap(gfs => readFile.map(_.map(gfs.enumerate(_))))

    def read(out: OutputStream)(implicit ec: ExecutionContext): Future[Option[Unit]] = readFile.flatMap {
      case Some(readFile) => gridfs.flatMap(_.readToOutputStream(readFile, out)).map(Some(_))
      case None => Future.successful(None)
    }
  }

  implicit def readFileWrapperToReadFile(readFileWrapper: ReadFileWrapper): Future[Option[BSONReadFile]] = readFileWrapper.readFile

}
