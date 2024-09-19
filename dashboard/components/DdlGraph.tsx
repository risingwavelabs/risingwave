import { theme } from "@chakra-ui/react"
import { tinycolor } from "@ctrl/tinycolor"
import * as d3 from "d3"
import { cloneDeep } from "lodash"
import { Fragment, useCallback, useEffect, useRef } from "react"
import {
  DdlBox,
  DdlBoxPosition,
  Edge,
  Enter,
  Position,
  generateDdlEdges,
  layoutItem,
} from "../lib/layout"

type DdlLayout = {
  id: string
  width: number
  height: number
  ddl_name: string
  schema_name: string
} & Position

const nodeRadius = 12
const ddlMarginY = nodeRadius * 3
const ddlDistanceX = nodeRadius * 2
const ddlDistanceY = nodeRadius * 2

export default function DdlGraph({
  ddlDependency,
  backPressures,
}: {
  ddlDependency: DdlBox[] // Ddl adjacency list, metadata
  backPressures?: Map<string, number> // relation_id_relation_id ->back_pressure_rate
}) {
  const svgRef = useRef<SVGSVGElement>(null)

  const ddlDependencyDagCallback = useCallback(() => {
    const ddlDependencyDag = cloneDeep(ddlDependency)

    const layoutDdlResult = new Map<string, any>()
    const includedDdlIds = new Set<string>()
    for (const ddlBox of ddlDependencyDag) {
      let ddlId = ddlBox.id
      layoutDdlResult.set(ddlId, {
        ddl_name: ddlBox.ddl_name,
        schema_name: ddlBox.schema_name,
      })
      includedDdlIds.add(ddlId)
    }

    const ddlLayout = layoutItem(
      ddlDependencyDag.map(({ width: _1, height: _2, id, ...data }) => {
        return { width: 100, height: 100, id, ...data }
      }),
      ddlDistanceX,
      ddlDistanceY
    )

    const layoutResult = ddlLayout.map(({ x, y, ...data }) => {
        return { x: x + 50, y: y + 50, ...data, }
    })

    let svgWidth = 0
    let svgHeight = 0
    ddlLayout.forEach(({ x, y }) => {
      svgHeight = Math.max(svgHeight, y + 100)
      svgWidth = Math.max(svgWidth, x + 100)
    })
    const edges = generateDdlEdges(ddlLayout)

    return {
      layoutResult,
      svgWidth,
      svgHeight,
      edges,
    }
  }, [ddlDependency])

  const {
    svgWidth,
    svgHeight,
    edges: ddlEdgeLayout,
    layoutResult: ddlLayout,
  } = ddlDependencyDagCallback()

  useEffect(() => {
    if (ddlLayout) {
      const svgNode = svgRef.current
      const svgSelection = d3.select(svgNode)

      // Ddls
      const applyDdl = (gSel: DdlSelection) => {
        gSel.attr("transform", ({ x, y }) => `translate(${x}, ${y})`)

        // Ddl text line 1 (ddl id)
        let text = gSel.select<SVGTextElement>(".text-frag-id")
        if (text.empty()) {
          text = gSel.append("text").attr("class", "text-frag-id")
        }

        text
          .attr("fill", "black")
          .text(({ ddl_name, schema_name }) => `${schema_name}.${ddl_name}`)
          .attr("font-family", "inherit")
          .attr("text-anchor", "middle")
          .attr("dy", ddlMarginY)
          .attr("fill", "black")
          .attr("font-size", 12)

        // Ddl bounding box
        let circle = gSel.select<SVGCircleElement>("circle")
        if (circle.empty()) {
          circle = gSel.append("circle")
        }

        circle.attr("r", 20).attr("fill", (_) => {
          const weight = "500"
          return theme.colors.gray[weight]
        })

        circle
          .attr("fill", "white")
          .attr("stroke-width", 1)
          .attr("stroke", theme.colors.gray[500])
      }

      const createDdl = (sel: Enter<DdlSelection>) =>
        sel.append("g").attr("class", "ddl").call(applyDdl)

      const ddlSelection = svgSelection
        .select<SVGGElement>(".ddls")
        .selectAll<SVGGElement, null>(".ddl")
        .data(ddlLayout)
      type DdlSelection = typeof ddlSelection

      ddlSelection.enter().call(createDdl)
      ddlSelection.call(applyDdl)
      ddlSelection.exit().remove()

      // Ddl Edges
      const edgeSelection = svgSelection
        .select<SVGGElement>(".ddl-edges")
        .selectAll<SVGGElement, null>(".ddl-edge")
        .data(ddlEdgeLayout)
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
        sel.append("g").attr("class", "ddl-edge").call(applyEdge)

      edgeSelection.enter().call(createEdge)
      edgeSelection.call(applyEdge)
      edgeSelection.exit().remove()
    }
  }, [ddlLayout, ddlEdgeLayout, backPressures])

  return (
    <Fragment>
      <svg ref={svgRef} width={`${svgWidth}px`} height={`${svgHeight}px`}>
        <g className="ddl-edges" />
        <g className="ddls" />
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
