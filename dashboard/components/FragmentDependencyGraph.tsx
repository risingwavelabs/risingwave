import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import { Dag, DagLink, DagNode, zherebko } from "d3-dag"
import { cloneDeep } from "lodash"
import { useCallback, useEffect, useRef, useState } from "react"
import { Enter, FragmentBox, Position } from "../lib/layout"

const nodeRadius = 5
const edgeRadius = 12

export default function FragmentDependencyGraph({
  fragmentDependency,
  svgWidth,
  selectedId,
  onSelectedIdChange,
}: {
  fragmentDependency: Dag<FragmentBox>
  svgWidth: number
  selectedId: string | undefined
  onSelectedIdChange: (id: string) => void | undefined
}) {
  const svgRef = useRef<SVGSVGElement>(null)
  const [svgHeight, setSvgHeight] = useState("0px")
  const MARGIN_X = 10
  const MARGIN_Y = 2

  const fragmentDependencyDagCallback = useCallback(() => {
    const layout = zherebko().nodeSize([
      nodeRadius * 2,
      (nodeRadius + edgeRadius) * 2,
      nodeRadius,
    ])
    const dag = cloneDeep(fragmentDependency)
    const { width, height } = layout(dag)
    return { width, height, dag }
  }, [fragmentDependency])

  const fragmentDependencyDag = fragmentDependencyDagCallback()

  useEffect(() => {
    const { width, height, dag } = fragmentDependencyDag

    // This code only handles rendering

    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)

    // How to draw edges
    const curveStyle = d3.curveMonotoneY
    const line = d3
      .line<Position>()
      .curve(curveStyle)
      .x(({ x }) => x + MARGIN_X)
      .y(({ y }) => y)

    const isSelected = (d: DagNode<FragmentBox>) => d.data.id === selectedId

    const edgeSelection = svgSelection
      .select(".edges")
      .selectAll<SVGPathElement, null>(".edge")
      .data(dag.links())
    type EdgeSelection = typeof edgeSelection

    const applyEdge = (sel: EdgeSelection) =>
      sel
        .attr("d", ({ points }: DagLink) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", (d) =>
          isSelected(d.source) || isSelected(d.target) ? 2 : 1
        )
        .attr("stroke", (d) =>
          isSelected(d.source) || isSelected(d.target)
            ? theme.colors.blue["500"]
            : theme.colors.gray["300"]
        )
    const createEdge = (sel: Enter<EdgeSelection>) =>
      sel.append("path").attr("class", "edge").call(applyEdge)
    edgeSelection.exit().remove()
    edgeSelection.enter().call(createEdge)
    edgeSelection.call(applyEdge)

    // Select nodes
    const nodeSelection = svgSelection
      .select(".nodes")
      .selectAll<SVGCircleElement, null>(".node")
      .data(dag.descendants())
    type NodeSelection = typeof nodeSelection

    const applyNode = (sel: NodeSelection) =>
      sel
        .attr("transform", (d) => `translate(${d.x! + MARGIN_X}, ${d.y})`)
        .attr("fill", (d) =>
          isSelected(d) ? theme.colors.blue["500"] : theme.colors.gray["500"]
        )

    const createNode = (sel: Enter<NodeSelection>) =>
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
      .selectAll<SVGTextElement, null>(".label")
      .data(dag.descendants())
    type LabelSelection = typeof labelSelection

    const applyLabel = (sel: LabelSelection) =>
      sel
        .text((d) => d.data.name)
        .attr("x", svgWidth - MARGIN_X)
        .attr("font-family", "inherit")
        .attr("text-anchor", "end")
        .attr("alignment-baseline", "middle")
        .attr("y", (d) => d.y!)
        .attr("fill", (d) =>
          isSelected(d) ? theme.colors.black["500"] : theme.colors.gray["500"]
        )
        .attr("font-weight", "600")
    const createLabel = (sel: Enter<LabelSelection>) =>
      sel.append("text").attr("class", "label").call(applyLabel)
    labelSelection.exit().remove()
    labelSelection.enter().call(createLabel)
    labelSelection.call(applyLabel)

    // Add overlays
    const overlaySelection = svgSelection
      .select(".overlays")
      .selectAll<SVGRectElement, null>(".overlay")
      .data(dag.descendants())
    type OverlaySelection = typeof overlaySelection

    const STROKE_WIDTH = 3
    const applyOverlay = (sel: OverlaySelection) =>
      sel
        .attr("x", STROKE_WIDTH)
        .attr(
          "height",
          nodeRadius * 2 + edgeRadius * 2 - MARGIN_Y * 2 - STROKE_WIDTH * 2
        )
        .attr("width", svgWidth - STROKE_WIDTH * 2)
        .attr(
          "y",
          (d) => d.y! - nodeRadius - edgeRadius + MARGIN_Y + STROKE_WIDTH
        )
        .attr("rx", 5)
        .attr("fill", theme.colors.gray["500"])
        .attr("opacity", 0)
        .style("cursor", "pointer")
    const createOverlay = (sel: Enter<OverlaySelection>) =>
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
            onSelectedIdChange(i.data.id)
          }
        })

    overlaySelection.exit().remove()
    overlaySelection.enter().call(createOverlay)
    overlaySelection.call(applyOverlay)

    setSvgHeight(`${height}px`)
  }, [
    fragmentDependency,
    selectedId,
    svgWidth,
    onSelectedIdChange,
    fragmentDependencyDag,
  ])

  return (
    <svg ref={svgRef} width={`${svgWidth}px`} height={svgHeight}>
      <g className="edges"></g>
      <g className="nodes"></g>
      <g className="labels"></g>
      <g className="overlays"></g>
    </svg>
  )
}
