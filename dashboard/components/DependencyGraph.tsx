import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import { Dag, zherebko } from "d3-dag"
import { DagLink, DagNode, Point } from "d3-dag/dist/dag"
import { useCallback, useEffect, useRef, useState } from "react"

const nodeRadius = 5
const edgeRadius = 12

export default function DependencyGraph({
  mvDependency,
  svgWidth,
  selectedId,
  onSelectedIdChange
}: {
  mvDependency: Dag
  svgWidth: number
  selectedId: string | undefined
  onSelectedIdChange: (id: string) => void | undefined
}) {
  const svgRef = useRef<any>()
  const [svgHeight, setSvgHeight] = useState("0px")
  const MARGIN_X = 10
  const MARGIN_Y = 2

  const mvDependencyDagCallback = useCallback(() => {
    const layout = zherebko().nodeSize([
      nodeRadius * 2,
      (nodeRadius + edgeRadius) * 2,
      nodeRadius,
    ])
    const { width, height } = layout(mvDependency)
    return { width, height, dag: mvDependency }
  }, [mvDependency])

  const mvDependencyDag = mvDependencyDagCallback()

  useEffect(() => {
    const { width, height, dag } = mvDependencyDag

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

    const isSelected = (d: any) => d.data.id === selectedId

    const edgeSelection = svgSelection
      .select(".edges")
      .selectAll(".edge")
      .data(dag.links())
    const applyEdge = (sel: any) =>
      sel
        .attr("d", ({ points }: DagLink) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", (d: any) => isSelected(d.source) || isSelected(d.target) ? 2 : 1)
        .attr("stroke", (d: any) =>
          isSelected(d.source) || isSelected(d.target)
            ? theme.colors.teal["500"]
            : theme.colors.gray["300"])
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
      sel
        .attr(
          "transform",
          ({ x, y }: Point) => `translate(${x + MARGIN_X}, ${y})`
        )
        .attr("fill", (d: any) =>
          isSelected(d)
            ? theme.colors.teal["500"]
            : theme.colors.gray["500"])

    const createNode = (sel: any) =>
      sel
        .append("circle")
        .attr("class", "node")
        .attr("r", nodeRadius)
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
        .attr("fill", (d: any) =>
          isSelected(d)
            ? theme.colors.black["500"]
            : theme.colors.gray["500"])
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

    const STROKE_WIDTH = 3
    const applyOverlay = (sel: any) =>
      sel
        .attr("x", STROKE_WIDTH)
        .attr("height", nodeRadius * 2 + edgeRadius * 2 - MARGIN_Y * 2 - STROKE_WIDTH * 2)
        .attr("width", svgWidth - STROKE_WIDTH * 2)
        .attr("y", (d: any) => d.y - nodeRadius - edgeRadius + MARGIN_Y + STROKE_WIDTH)
        .attr("rx", 5)
        .attr("fill", theme.colors.gray["500"])
        .attr("opacity", 0)
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
        .on("mousedown", function (d, i) {
          d3.select(this)
            .transition()
            .duration(parseInt(theme.transition.duration.normal))
            .attr("opacity", ".20")
        })
        .on("mouseup", function (d, i) {
          d3.select(this)
            .transition()
            .duration(parseInt(theme.transition.duration.normal))
            .attr("opacity", ".10")
        })
        .on("click", function (d, i) {
          if (onSelectedIdChange) {
            onSelectedIdChange((i.data as any).id)
          }
        })

    overlaySelection.exit().remove()
    overlaySelection.enter().call(createOverlay)
    overlaySelection.call(applyOverlay)

    setSvgHeight(`${height}px`)
  }, [mvDependency, selectedId, svgWidth, onSelectedIdChange])

  return (
    <svg ref={svgRef} width={`${svgWidth}px`} height={svgHeight}>
      <g className="edges"></g>
      <g className="nodes"></g>
      <g className="labels"></g>
      <g className="overlays"></g>
    </svg>
  )
}
