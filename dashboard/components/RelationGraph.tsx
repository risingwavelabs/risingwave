import { theme } from "@chakra-ui/react"
import { tinycolor } from "@ctrl/tinycolor"
import * as d3 from "d3"
import { cloneDeep } from "lodash"
import { Fragment, useCallback, useEffect, useRef } from "react"
import {
  RelationBox,
  Edge,
  Enter,
  Position,
  generateRelationBackPressureEdges,
  layoutItem,
} from "../lib/layout"

const nodeRadius = 12
const relationLayoutX = nodeRadius * 8
const relationLayoutY = nodeRadius * 8
const relationNameX = nodeRadius * 4
const relationNameY = nodeRadius * 7
const relationMarginX = nodeRadius * 2
const relationMarginY = nodeRadius * 2

export default function RelationGraph({
  relationDependency,
  backPressures,
}: {
  relationDependency: RelationBox[] // Relation adjacency list, metadata
  backPressures?: Map<string, number> // relationId-relationId->back_pressure_rate
}) {
  const svgRef = useRef<SVGSVGElement>(null)

  const relationDependencyDagCallback = useCallback(() => {
    const relationDependencyDag = cloneDeep(relationDependency)

    const layoutRelationResult = new Map<string, any>()
    const includedRelationIds = new Set<string>()
    for (const relationBox of relationDependencyDag) {
      let relationId = relationBox.id
       layoutRelationResult.set(relationId, {
        relationName: relationBox.relationName,
        schemaName: relationBox.schemaName,
      })
      includedRelationIds.add(relationId)
    }

    const relationLayout = layoutItem(
      relationDependencyDag.map(({ width: _1, height: _2, id, ...data }) => {
        return { width: relationLayoutX, height: relationLayoutY, id, ...data }
      }),
      relationMarginX,
      relationMarginY
    )

    let svgWidth = 0
    let svgHeight = 0
    relationLayout.forEach(({ x, y }) => {
      svgHeight = Math.max(svgHeight, y + relationLayoutX)
      svgWidth = Math.max(svgWidth, x + relationLayoutY)
    })
    const edges = generateRelationBackPressureEdges(relationLayout)

    return {
      layoutResult: relationLayout,
      svgWidth,
      svgHeight,
      edges,
    }
  }, [relationDependency])

  const {
    svgWidth,
    svgHeight,
    edges: relationEdgeLayout,
    layoutResult: relationLayout,
  } = relationDependencyDagCallback()

  useEffect(() => {
    if (relationLayout) {
      const svgNode = svgRef.current
      const svgSelection = d3.select(svgNode)

      // Relations
      const applyRelation = (gSel: RelationSelection) => {
        gSel.attr("transform", ({ x, y }) => `translate(${x}, ${y})`)

        // Render relation name
        {
          let text = gSel.select<SVGTextElement>(".text-frag-id")
          if (text.empty()) {
            text = gSel.append("text").attr("class", "text-frag-id")
          }

          text
            .attr("fill", "black")
            .text(({ relationName, schemaName }) => `${schemaName}.${relationName}`)
            .attr("font-family", "inherit")
            .attr("text-anchor", "middle")
            .attr("dx", relationNameX)
            .attr("dy", relationNameY)
            .attr("fill", "black")
            .attr("font-size", 12)
        }

        // Render relation node
        {
          let circle = gSel.select<SVGCircleElement>("circle")
          if (circle.empty()) {
            circle = gSel.append("circle")
          }

          circle.attr("r", 20).attr("fill", (_) => {
            const weight = "500"
            return theme.colors.gray[weight]
          })

          circle
            .attr("cx", relationLayoutX / 2)
            .attr("cy", relationLayoutY / 2)
            .attr("fill", "white")
            .attr("stroke-width", 1)
            .attr("stroke", theme.colors.gray[500])
        }
      }

      const createRelation = (sel: Enter<RelationSelection>) =>
        sel.append("g").attr("class", "relation").call(applyRelation)

      const relationSelection = svgSelection
        .select<SVGGElement>(".relations")
        .selectAll<SVGGElement, null>(".relation")
        .data(relationLayout)
      type RelationSelection = typeof relationSelection

      relationSelection.enter().call(createRelation)
      // TODO(kwannoel): Is this even needed? I commented it out.
      // relationSelection.call(applyRelation)
      relationSelection.exit().remove()

      // Relation Edges
      const edgeSelection = svgSelection
        .select<SVGGElement>(".relation-edges")
        .selectAll<SVGGElement, null>(".relation-edge")
        .data(relationEdgeLayout)
      type EdgeSelection = typeof edgeSelection

      const curveStyle = d3.curveMonotoneX

      const line = d3
        .line<Position>()
        .curve(curveStyle)
        .x(({ x }) => x)
        .y(({ y }) => y)

      const applyEdge = (gSel: EdgeSelection) => {
        // Edge line
        let path = gSel.select<SVGPathElement>("path")
        if (path.empty()) {
          path = gSel.append("path")
        }

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

        path
          .attr("d", ({ points }) => line(points))
          .attr("fill", "none")
          .attr("stroke-width", width)
          .attr("stroke", color)

        // Tooltip for back pressure rate
        let title = gSel.select<SVGTitleElement>("title")
        if (title.empty()) {
          title = gSel.append<SVGTitleElement>("title")
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

        return gSel
      }
      const createEdge = (sel: Enter<EdgeSelection>) =>
        sel.append("g").attr("class", "relation-edge").call(applyEdge)

      edgeSelection.enter().call(createEdge)
      edgeSelection.call(applyEdge)
      edgeSelection.exit().remove()
    }
  }, [relationLayout, relationEdgeLayout, backPressures])

  return (
    <Fragment>
      <svg ref={svgRef} width={`${svgWidth}px`} height={`${svgHeight}px`}>
        <g className="relation-edges" />
        <g className="relations" />
      </svg>
    </Fragment>
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
