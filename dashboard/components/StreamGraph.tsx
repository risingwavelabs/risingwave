/*
 * Copyright 2022 Singularity Data
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
 *
 */

import * as d3 from "d3"
import { useEffect, useRef } from "react"
import { layout } from "../lib/layout"
import { ActorBox } from "./FragmentGraph"

export function StreamGraph() {
  const svgRef = useRef<any>()

  let data = new Array<ActorBox>()
  const appendData = (
    id: string,
    width: number,
    height: number,
    parentIds: string[]
  ) => {
    data.push({ id: id, name: id, order: 0, width, height, parentIds })
  }
  appendData("1", 200, 100, [])
  appendData("2", 200, 100, ["1"])
  appendData("3", 150, 100, ["1"])
  appendData("4", 200, 150, ["2"])
  appendData("5", 200, 100, ["3", "4"])
  appendData("6", 200, 100, ["2", "4"])

  let layoutMap = layout(data, 50, 100)

  useEffect(() => {
    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)
    const g = svgSelection.select(".box")
    let marginTop = 50
    for (let item of layoutMap) {
      let ab = item[0],
        x = item[1][0],
        y = item[1][1]
      g.append("rect")
        .attr("x", x)
        .attr("y", y + marginTop)
        .attr("width", ab.width)
        .attr("height", ab.height)
        .attr("fill", "gray")
      g.append("text")
        .attr("x", x)
        .attr("y", y + marginTop)
        .attr("fill", "black")
        .text(ab.id)
    }
    console.log(layoutMap)
  }, [layoutMap])

  return (
    <>
      <svg ref={svgRef} width="800px" height="800px">
        <g className="box" />
      </svg>
    </>
  )
}
