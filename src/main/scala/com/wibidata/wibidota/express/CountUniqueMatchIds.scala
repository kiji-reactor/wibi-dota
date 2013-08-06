/* Copyright 2013 WibiData, Inc.
*
* See the NOTICE file distributed with this work for additional
* information regarding copyright ownership.
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

package com.wibidata.wibidota.express

import com.twitter.scalding.{JsonLine, Args, Csv}
import org.kiji.express.flow.KijiJob

/**
 * Counts the number of unique values field 'match_id' take in a file of
 * json lines.
 *
 * @param args, command line arguements with flags
 * --file, where the input file is located
 * --output, where to dump the resulting count
 */
class CountUniqueMatchIds(args: Args) extends KijiJob(args) {
  JsonLine(args("file"), 'match_id).mapTo('match_id)
  {line : String => line.toLong}.unique('match_id).groupAll{_.size}.write(Csv(args("output")))

}
