/*
 * Copyright 2020 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.{ByteOrderMark, CsvFormatting, CsvParsing, CsvQuotingStyle}
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult}
import akka.testkit.TestKit
import akka.util.ByteString
import model.Anonymize
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpecLike}

import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class AnonymiseFileSpec extends TestKit(ActorSystem("ff")) with WordSpecLike with Matchers
  with BeforeAndAfterEach {

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val csvList = List(
    "tblCaseClassMeth_csv")

  private val anonymizedCsvList = csvList.filterNot(_ == "historicCases_csv")
  private val mockCsvRowCount = 136

  private def mockCsv(headers: List[String], rowCount: Int = mockCsvRowCount): String = {
    var result = headers.mkString(",")
    result += "\n"

    for (row <- 0 until rowCount) {
      var rowFields = ListBuffer[String]()
      for (col <- headers.indices) {
        rowFields += mockCsvField(row, col)
      }

      result += rowFields.mkString(",")
      result += "\n"
    }

    result
  }

  private def mockCsvField(row: Int, col: Int): String = s"$row-$col"

  "anonymiseData /" should {


    "return valid anonymised files" in {

      val headers = "CaseNo,BERTISearch,EBTISearch,Justification,CommercialDenomenation,GoodsDescription,Exclusions,LGCExpertAdvice,OTCCommodityCode,ApplicantsCommodityCode,OTCImage,DescriptionIncluded,BrochureIncluded,PhotoIncluded,SampleIncluded,OtherIncluded,CombinedNomenclature,TARICCode,TARICAdditionalCode1,TARICAdditionalCode2,NationalAdditionalCode".split(",").toList
      val path = {
        val file = Files.createTempFile("", ".csv")
        Files.write(file,mockCsv(headers).getBytes("UTF-8"))
        file
      }

      // Read the output file

      val outputLines = Await.result(anonymiseData(path)
        .via(CsvParsing.lineScanner())
        .map(_.map(_.utf8String))
        .runWith(Sink.seq), 1 minute)

      val outputHeaders = outputLines.head
      val outputDataRows = outputLines.tail

      // Determine if a given field is anonymized (would be better if this could be obtained from a map)
      val isAnonymized = headers.map(header => (header, Anonymize.anonymize("tblCaseClassMeth_csv", Map((header -> "TEST")))(header) != "TEST")).toMap

      // Ensure at least 1 field is anonymized
      isAnonymized.values.count(_ == true) >= 1 shouldBe true

      outputHeaders shouldBe headers

      for (row <- 0 until mockCsvRowCount) {
        for (col <- headers.indices) {
          if (isAnonymized(headers(col))) {
            outputDataRows(row)(col) shouldNot equal(mockCsvField(row, col))
          } else {
            outputDataRows(row)(col) shouldBe mockCsvField(row, col)
          }
        }
      }
    }

  }

  private def anonymiseData(path: Path): Source[ByteString, Future[IOResult]] = {

    var headers: Option[List[String]] = None

    val file = FileIO.fromPath(path)

    val res = file
      .via(Flow.fromFunction {
        file =>
          //This is done because the byte order mark (BOM) causes problems with first column header
          if (file.startsWith(ByteOrderMark.UTF_8)) {
            file.drop(ByteOrderMark.UTF_8.length).dropWhile(b => b.toChar.isWhitespace)
          } else {
            file.dropWhile(b => b.toChar.isWhitespace)
          }
      })
      .via(CsvParsing.lineScanner())
      .map(_.map(_.utf8String))
      .filter(_.mkString.trim.nonEmpty) // ignore blank lines in CSV
      .map { list =>
        headers match {
          case None =>
            headers = Some(list)
            (list, None)
          case Some(headers) =>
            (headers, Some(list))
        }
      }
      .map {
        case (headers, None) =>
          (headers, None)
        case (headers, Some(data)) =>
          val dataByColumn: Map[String, String] = ListMap(headers.zip(data): _*)
          val anonymized: Map[String, String] = Anonymize.anonymize("tblCaseClassMeth_csv", dataByColumn)
          (headers, Some(headers.map(col => anonymized(col))))
      }
      .flatMapMerge(1, {
        case (headers, None) =>
          Source.single(headers).via(CsvFormatting.format(quotingStyle = CsvQuotingStyle.Always))
        case (_, Some(data)) =>
          Source.single(data).via(CsvFormatting.format(quotingStyle = CsvQuotingStyle.Always))
      })

    res
  }

}
