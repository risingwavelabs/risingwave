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

import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import { cloneDeep } from "lodash"
import { useCallback, useEffect, useRef } from "react"
import { layout } from "../lib/layout"
import { ActorBox } from "./FragmentGraph"

function boundBox(actorPosition: Map<ActorBox, [number, number]>): {
  width: number
  height: number
} {
  let width = 0
  let height = 0
  for (const [box, [x, y]] of actorPosition) {
    width = Math.max(width, x + box.width)
    height = Math.max(height, y + box.height)
  }
  return { width, height }
}

const layerMargin = 50
const rowMargin = 200

function flipLayout(
  fragments: Array<ActorBox>,
  layerMargin: number,
  rowMargin: number
): Map<ActorBox, [number, number]> {
  const fragments_ = cloneDeep(fragments)
  for (let fragment of fragments_) {
    ;[fragment.width, fragment.height] = [fragment.height, fragment.width]
  }
  const actorPosition = layout(fragments_, rowMargin, layerMargin)
  for (const [fragment, [x, y]] of actorPosition) {
    actorPosition.set(fragment, [y, x])
  }
  return actorPosition
}

type ActorLayout = [ActorBox, [number, number]]
const layoutMargin = 100

export function StreamGraph({
  nodes,
  selectedId,
}: {
  nodes: ActorBox[]
  selectedId?: string
}) {
  const svgRef = useRef<any>()

  const layoutMapCallback = useCallback(() => {
    const layoutMap = flipLayout(nodes, layerMargin, rowMargin)
    const links = []
    const fragmentMap = new Map<string, [ActorBox, { x: number; y: number }]>()
    for (const [fragment, [x, y]] of layoutMap) {
      fragmentMap.set(fragment.id, [
        fragment,
        {
          x: x + layoutMargin + fragment.width / 2,
          y: y + layoutMargin + fragment.height / 2,
        },
      ])
    }
    for (const [fragment, [x, y]] of layoutMap) {
      for (const parentId of fragment.parentIds) {
        links.push({
          points: [
            {
              x: x + layoutMargin + fragment.width / 2,
              y: y + layoutMargin + fragment.height / 2,
            },
            fragmentMap.get(parentId)![1],
          ],
          source: fragment.id,
          target: parentId,
        })
      }
    }
    const { width, height } = boundBox(layoutMap)
    return {
      layoutMap: Array.from(layoutMap.entries()).map(
        ([fragment, [x, y]]: ActorLayout) => [
          fragment,
          [x + layoutMargin, y + layoutMargin],
        ]
      ),
      links,
      width: width + rowMargin + layoutMargin * 2,
      height: height + layerMargin + layoutMargin * 2,
    }
  }, [nodes])

  const { layoutMap, width, height, links } = layoutMapCallback()

  useEffect(() => {
    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)

    const curveStyle = d3.curveMonotoneY

    const line = d3
      .line<{ x: number; y: number }>()
      .curve(curveStyle)
      .x(({ x }) => x)
      .y(({ y }) => y)

    const edgeSelection = svgSelection
      .select(".edges")
      .selectAll(".edge")
      .data(links)

    const isSelected = (id: string) => id === selectedId

    const applyEdge = (sel: any) =>
      sel
        .attr("d", ({ points }: any) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", 1)
        .attr("stroke-width", (d: any) =>
          isSelected(d.source) || isSelected(d.target) ? 2 : 1
        )
        .attr("opacity", (d: any) =>
          isSelected(d.source) || isSelected(d.target) ? 1 : 0.5
        )
        .attr("stroke", (d: any) =>
          isSelected(d.source) || isSelected(d.target)
            ? theme.colors.teal["500"]
            : theme.colors.gray["300"]
        )

    const createEdge = (sel: any) =>
      sel.append("path").attr("class", "edge").call(applyEdge)
    edgeSelection.exit().remove()
    edgeSelection.enter().call(createEdge)
    edgeSelection.call(applyEdge)

    const applyNode = (g: any) => {
      g.attr(
        "transform",
        ([fragment, [x, y]]: ActorLayout) =>
          `translate(${x + fragment.width / 2},${y + fragment.height / 2})`
      )

      let circle = g.select("circle")
      if (circle.empty()) {
        circle = g.append("circle")
      }

      circle
        .attr("r", ([fragment, _]: ActorLayout) => fragment.width / 2)
        .style("cursor", "pointer")
        .attr("fill", ([fragment, _]: ActorLayout) =>
          isSelected(fragment.id)
            ? theme.colors.teal["500"]
            : theme.colors.gray["500"]
        )

      let text = g.select("text")
      if (text.empty()) {
        text = g.append("text")
      }

      text
        .attr("fill", "black")
        .text(([fragment, _]: ActorLayout) => fragment.name)
        .attr("font-family", "inherit")
        .attr("text-anchor", "middle")
        .attr("dy", ([fragment, _]: ActorLayout) => fragment.width)
        .attr("fill", "black")
        .attr("font-size", 12)
        .attr("transform", "rotate(-8)")

      return g
    }

    const createNode = (sel: any) =>
      sel.append("g").attr("class", "node").call(applyNode)

    const g = svgSelection.select(".boxes")
    const nodeSelection = g.selectAll(".node").data(layoutMap)
    nodeSelection.enter().call(createNode)
    nodeSelection.call(applyNode)
    nodeSelection.exit().remove()
  }, [layoutMap, links, selectedId])

  return (
    <>
      <svg ref={svgRef} width={`${width}px`} height={`${height}px`}>
        <g className="edges" />
        <g className="boxes" />
      </svg>
    </>
  )
}
