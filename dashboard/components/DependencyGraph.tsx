import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import { Dag, zherebko } from "d3-dag"
import { DagLink, DagNode, Point } from "d3-dag/dist/dag"
import { useEffect, useRef, useState } from "react"

const nodeRadius = 5
const edgeRadius = 12

export default function DependencyGraph({
  mvDependency,
  svgWidth,
  selectedId,
}: {
  mvDependency: Dag
  svgWidth: number
  selectedId: string | undefined
}) {
  const svgRef = useRef<any>()
  const [svgHeight, setSvgHeight] = useState("0px")
  const MARGIN_X = 10
  const MARGIN_Y = 2

  useEffect(() => {
    const layout = zherebko().nodeSize([
      nodeRadius * 2,
      (nodeRadius + edgeRadius) * 2,
      nodeRadius,
    ])

    const { width, height } = layout(mvDependency)
    const dag = mvDependency

    // This code only handles rendering

    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)

    // How to draw edges
    const curveStyle = d3.curveMonotoneY
    const line = d3
      .line<Point>()
      .curve(curveStyle)
      .x(({ x }) => x + MARGIN_X)
      .y(({ y }) => y)

    const edgeSelection = svgSelection
      .select(".edges")
      .selectAll(".edge")
      .data(dag.links())
    const applyEdge = (sel: any) =>
      sel
        .attr("d", ({ points }: DagLink) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", 1)
        .attr("stroke", theme.colors.gray["500"])
    const createEdge = (sel: any) =>
      sel.append("path").attr("class", "edge").call(applyEdge)
    edgeSelection.exit().remove()
    edgeSelection.enter().call(createEdge)
    edgeSelection.call(applyEdge)

    // Select nodes
    const nodeSelection = svgSelection
      .select(".nodes")
      .selectAll(".node")
      .data(dag.descendants())
    const applyNode = (sel: any) =>
      sel.attr(
        "transform",
        ({ x, y }: Point) => `translate(${x + MARGIN_X}, ${y})`
      )
    const createNode = (sel: any) =>
      sel
        .append("circle")
        .attr("class", "node")
        .attr("r", nodeRadius)
        .attr("fill", theme.colors.teal["500"])
        .call(applyNode)
    nodeSelection.exit().remove()
    nodeSelection.enter().call(createNode)
    nodeSelection.call(applyNode)

    // Add text to nodes
    const labelSelection = svgSelection
      .select(".labels")
      .selectAll(".label")
      .data(dag.descendants())

    const applyLabel = (sel: any) =>
      sel
        .text((d: any) => d.data.name)
        .attr("x", svgWidth - MARGIN_X)
        .attr("font-family", "inherit")
        .attr("text-anchor", "end")
        .attr("alignment-baseline", "middle")
        .attr("y", (d: any) => d.y)
        .attr("fill", theme.colors.gray["500"])
        .attr("font-weight", "600")
    const createLabel = (sel: any) =>
      sel.append("text").attr("class", "label").call(applyLabel)
    labelSelection.exit().remove()
    labelSelection.enter().call(createLabel)
    labelSelection.call(applyLabel)

    // Add overlays
    const overlaySelection = svgSelection
      .select(".overlays")
      .selectAll(".overlay")
      .data(dag.descendants())

    const applyOverlay = (sel: any) =>
      sel
        .attr("x", 0)
        .attr("height", nodeRadius * 2 + edgeRadius * 2 - MARGIN_Y * 2)
        .attr("width", svgWidth)
        .attr("y", (d: any) => d.y - nodeRadius - edgeRadius + MARGIN_Y)
        .attr("rx", 5)
        .attr("fill", theme.colors.gray["500"])
        .attr("opacity", "0")
        .style("cursor", "pointer")
    const createOverlay = (
      sel: d3.Selection<
        d3.EnterElement,
        DagNode<unknown, unknown>,
        d3.BaseType,
        unknown
      >
    ) =>
      sel
        .append("rect")
        .attr("class", "overlay")
        .call(applyOverlay)
        .on("mouseover", function (d, i) {
          d3.select(this)
            .transition()
            .duration(parseInt(theme.transition.duration.normal))
            .attr("opacity", ".10")
        })
        .on("mouseout", function (d, i) {
          d3.select(this)
            .transition()
            .duration(parseInt(theme.transition.duration.normal))
            .attr("opacity", "0")
        })

    overlaySelection.exit().remove()
    overlaySelection.enter().call(createOverlay)
    overlaySelection.call(applyOverlay)

    setSvgHeight(`${height}px`)
  }, [mvDependency, selectedId, svgWidth])

  return (
    <svg ref={svgRef} width={`${svgWidth}px`} height={svgHeight}>
      <g className="edges"></g>
      <g className="nodes"></g>
      <g className="labels"></g>
      <g className="overlays"></g>
    </svg>
  )
}
