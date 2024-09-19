/*
 * Copyright 2024 RisingWave Labs
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
 *
 */

import { theme } from "@chakra-ui/react"
import { tinycolor } from "@ctrl/tinycolor"
import * as d3 from "d3"
import { useCallback, useEffect, useRef } from "react"
import {
  Relation,
  relationIsStreamingJob,
  relationType,
  relationTypeTitleCase,
} from "../lib/api/streaming"
import {
  Edge,
  Enter,
  Position,
  RelationPoint,
  RelationPointPosition,
  flipLayoutRelation,
  generateRelationEdges,
} from "../lib/layout"
import { CatalogModal, useCatalogModal } from "./CatalogModal"

function boundBox(
  relationPosition: RelationPointPosition[],
  nodeRadius: number
): {
  width: number
  height: number
} {
  let width = 0
  let height = 0
  for (const { x, y } of relationPosition) {
    width = Math.max(width, x + nodeRadius)
    height = Math.max(height, y + nodeRadius)
  }
  return { width, height }
}

const layerMargin = 50
const rowMargin = 50
export const nodeRadius = 12
const layoutMargin = 50

export default function RelationDependencyGraph({
  nodes,
  selectedId,
  setSelectedId,
  backPressures,
}: {
  nodes: RelationPoint[]
  selectedId: string | undefined
  setSelectedId: (id: string) => void
  backPressures: Map<string, number> // relationId-relationId->back_pressure_rate})
}) {
  const [modalData, setModalId] = useCatalogModal(nodes.map((n) => n.relation))

  const svgRef = useRef<SVGSVGElement>(null)

  const layoutMapCallback = useCallback(() => {
    const layoutMap = flipLayoutRelation(
      nodes,
      layerMargin,
      rowMargin,
      nodeRadius
    ).map(
      ({ x, y, ...data }) =>
        ({
          x: x + layoutMargin,
          y: y + layoutMargin,
          ...data,
        } as RelationPointPosition)
    )
    const links = generateRelationEdges(layoutMap)
    const { width, height } = boundBox(layoutMap, nodeRadius)
    return {
      layoutMap,
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
      .line<Position>()
      .curve(curveStyle)
      .x(({ x }) => x)
      .y(({ y }) => y)

    const edgeSelection = svgSelection
      .select(".edges")
      .selectAll<SVGPathElement, null>(".edge")
      .data(links)
    type EdgeSelection = typeof edgeSelection

    const isSelected = (id: string) => id === selectedId

    const applyEdge = (sel: EdgeSelection) => {
      const color = (d: Edge) => {
        if (backPressures) {
          let value = backPressures.get(`${d.target}_${d.source}`)
          if (value) {
            return backPressureColor(value)
          }
        }

        return theme.colors.gray["300"]
      }

      const width = (d: Edge) => {
        if (backPressures) {
          let value = backPressures.get(`${d.target}_${d.source}`)
          if (value) {
            return backPressureWidth(value)
          }
        }

        return 2
      }

      sel
        .attr("d", ({ points }) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", width)
        .attr("stroke", color)
        .attr("opacity", (d) =>
          isSelected(d.source) || isSelected(d.target) ? 1 : 0.5
        )

      // Tooltip for back pressure rate
      let title = sel.select<SVGTitleElement>("title")
      if (title.empty()) {
        title = sel.append<SVGTitleElement>("title")
      }

      const text = (d: Edge) => {
        if (backPressures) {
          let value = backPressures.get(`${d.target}_${d.source}`)
          if (value) {
            return `${value.toFixed(2)}%`
          }
        }

        return ""
      }

      title.text(text)

      return sel
    }

    const createEdge = (sel: Enter<EdgeSelection>) =>
      sel.append("path").attr("class", "edge").call(applyEdge)
    edgeSelection.exit().remove()
    edgeSelection.enter().call(createEdge)
    edgeSelection.call(applyEdge)

    const applyNode = (g: NodeSelection) => {
      g.attr("transform", ({ x, y }) => `translate(${x},${y})`)

      // Circle
      let circle = g.select<SVGCircleElement>("circle")
      if (circle.empty()) {
        circle = g.append("circle")
      }

      circle.attr("r", nodeRadius).attr("fill", ({ id, relation }) => {
        const weight = relationIsStreamingJob(relation) ? "500" : "400"
        return isSelected(id)
          ? theme.colors.blue[weight]
          : theme.colors.gray[weight]
      })

      // Relation name
      let text = g.select<SVGTextElement>(".text")
      if (text.empty()) {
        text = g.append("text").attr("class", "text")
      }

      text
        .attr("fill", "black")
        .text(({ name }) => name)
        .attr("font-family", "inherit")
        .attr("text-anchor", "middle")
        .attr("dy", nodeRadius * 2)
        .attr("font-size", 12)
        .attr("transform", "rotate(-8)")

      // Relation type
      let typeText = g.select<SVGTextElement>(".type")
      if (typeText.empty()) {
        typeText = g.append("text").attr("class", "type")
      }

      const relationTypeAbbr = (relation: Relation) => {
        const type = relationType(relation)
        if (type === "SINK") {
          return "K"
        } else {
          return type.charAt(0)
        }
      }

      typeText
        .attr("fill", "white")
        .text(({ relation }) => `${relationTypeAbbr(relation)}`)
        .attr("font-family", "inherit")
        .attr("text-anchor", "middle")
        .attr("dy", nodeRadius * 0.5)
        .attr("font-size", 16)
        .attr("font-weight", "bold")

      // Relation type tooltip
      let typeTooltip = g.select<SVGTitleElement>("title")
      if (typeTooltip.empty()) {
        typeTooltip = g.append<SVGTitleElement>("title")
      }

      typeTooltip.text(
        ({ relation }) =>
          `${relation.name} (${relationTypeTitleCase(relation)})`
      )

      // Relation modal
      g.style("cursor", "pointer").on("click", (_, { relation, id }) => {
        setSelectedId(id)
        setModalId(relation.id)
      })

      return g
    }

    const createNode = (sel: Enter<NodeSelection>) =>
      sel.append("g").attr("class", "node").call(applyNode)

    const g = svgSelection.select(".boxes")
    const nodeSelection = g
      .selectAll<SVGGElement, null>(".node")
      .data(layoutMap)
    type NodeSelection = typeof nodeSelection

    nodeSelection.enter().call(createNode)
    nodeSelection.call(applyNode)
    nodeSelection.exit().remove()
  }, [layoutMap, links, selectedId, setModalId, setSelectedId, backPressures])

  return (
    <>
      <svg ref={svgRef} width={`${width}px`} height={`${height}px`}>
        <g className="edges" />
        <g className="boxes" />
      </svg>
      <CatalogModal modalData={modalData} onClose={() => setModalId(null)} />
    </>
  )
}

/**
 * The color for the edge with given back pressure value.
 *
 * @param value The back pressure rate, between 0 and 100.
 */
function backPressureColor(value: number) {
  const colorRange = [
    theme.colors.green["100"],
    theme.colors.green["300"],
    theme.colors.yellow["400"],
    theme.colors.orange["500"],
    theme.colors.red["700"],
  ].map((c) => tinycolor(c))

  value = Math.max(value, 0)
  value = Math.min(value, 100)

  const step = colorRange.length - 1
  const pos = (value / 100) * step
  const floor = Math.floor(pos)
  const ceil = Math.ceil(pos)

  const color = tinycolor(colorRange[floor])
    .mix(tinycolor(colorRange[ceil]), (pos - floor) * 100)
    .toHexString()

  return color
}

/**
 * The width for the edge with given back pressure value.
 *
 * @param value The back pressure rate, between 0 and 100.
 */
function backPressureWidth(value: number) {
  value = Math.max(value, 0)
  value = Math.min(value, 100)

  return 15 * (value / 100) + 2
}
