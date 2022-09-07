import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import { cloneDeep } from "lodash"
import { useCallback, useEffect, useRef, useState } from "react"

interface Point {
  x: number
  y: number
}

export interface ActorBox {
  id: string
  name: string
  order: number // preference order, actor box with larger order will be placed at right
  width: number
  height: number
  parentIds: string[]
}

function treeLayoutFlip<Datum>(
  root: d3.HierarchyNode<Datum>,
  { nodeRadius, dx, dy }: { nodeRadius: number; dx: number; dy: number }
): d3.HierarchyPointNode<Datum> {
  const tree = d3.tree<Datum>().nodeSize([dy, dx])

  // Flip x, y
  const treeRoot = tree(root)

  // Flip back x, y
  treeRoot.each((d: Point) => ([d.x, d.y] = [d.y, d.x]))

  // LTR -> RTL
  treeRoot.each((d: Point) => (d.x = -d.x))

  return treeRoot
}

function boundBox<Datum>(
  root: d3.HierarchyPointNode<Datum>,
  {
    margin: { top, bottom, left, right },
  }: { margin: { top: number; bottom: number; left: number; right: number } }
): { width: number; height: number } {
  let x0 = Infinity
  let x1 = -x0
  let y0 = Infinity
  let y1 = -y0

  root.each((d) => (x1 = d.x > x1 ? d.x : x1))
  root.each((d) => (x0 = d.x < x0 ? d.x : x0))
  root.each((d) => (y1 = d.y > y1 ? d.y : y1))
  root.each((d) => (y0 = d.y < y0 ? d.y : y0))

  x0 -= left
  x1 += right
  y0 -= top
  y1 += bottom

  root.each((d) => (d.x = d.x - x0))
  root.each((d) => (d.y = d.y - y0))

  return { width: x1 - x0, height: y1 - y0 }
}

const nodeRadius = 12
const nodeMarginX = nodeRadius * 6
const nodeMarginY = nodeRadius * 4
const actorMarginX = nodeRadius * 2
const actorMarginY = nodeRadius * 2

export default function FragmentGraph({
  planNodeDependency,
}: {
  planNodeDependency: d3.HierarchyNode<any>
}) {
  const svgRef = useRef<any>()
  const [svgWidth, setSvgWidth] = useState(0)
  const [svgHeight, setSvgHeight] = useState(0)

  const planNodeDependencyDagCallback = useCallback(() => {
    const root = cloneDeep(planNodeDependency)
    const layoutRoot = treeLayoutFlip(root, {
      nodeRadius,
      dx: nodeMarginX,
      dy: nodeMarginY,
    })
    let { width, height } = boundBox(layoutRoot, {
      margin: {
        left: nodeRadius * 4 + actorMarginX,
        right: nodeRadius * 4 + actorMarginX,
        top: nodeRadius * 3 + actorMarginY,
        bottom: nodeRadius * 4 + actorMarginY,
      },
    })
    return { layoutRoot, width, height }
  }, [planNodeDependency])

  const planNodeDependencyDag = planNodeDependencyDagCallback()

  useEffect(() => {
    const { layoutRoot, width, height } = planNodeDependencyDag

    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)

    // How to draw edges
    const treeLink = d3
      .linkHorizontal<any, Point>()
      .x((d: Point) => d.x)
      .y((d: Point) => d.y)

    // Actor bounding box
    svgSelection
      .select(".bounding-box")
      .attr("width", width - actorMarginX * 2)
      .attr("height", height - actorMarginY * 2)
      .attr("x", actorMarginX)
      .attr("y", actorMarginY)
      .attr("fill", "white")
      .attr("stroke-width", 1)
      .attr("rx", 5)
      .attr("stroke", theme.colors.gray[500])

    svgSelection
      .select(".links")
      .attr("fill", "none")
      .attr("stroke", theme.colors.gray[700])
      .attr("stroke-width", 1.5)
      .selectAll("path")
      .data(layoutRoot.links())
      .join("path")
      .attr("d", treeLink)

    const nodes = svgSelection
      .select(".nodes")
      .attr("stroke-linejoin", "round")
      .attr("stroke-width", 3)

    const applyActorNode = (g: any) => {
      g.attr("transform", (d: any) => `translate(${d.x},${d.y})`)

      let circle = g.select("circle")
      if (circle.empty()) {
        circle = g.append("circle")
      }

      circle
        .attr("fill", theme.colors.teal[500])
        .attr("r", nodeRadius)
        .style("cursor", "pointer")

      let text = g.select("text")
      if (text.empty()) {
        text = g.append("text")
      }

      text
        .attr("fill", "black")
        .text((d: any) => d.data.name)
        .attr("font-family", "inherit")
        .attr("text-anchor", "middle")
        .attr("dy", nodeRadius * 1.8)
        .attr("fill", "black")
        .attr("font-size", 12)
        .attr("transform", "rotate(-8)")

      return g
    }

    const createActorNode = (sel: any) =>
      sel.append("g").attr("class", "actor-node").call(applyActorNode)

    const actorNodeSelection = nodes
      .selectAll(".actor-node")
      .data(layoutRoot.descendants())
    actorNodeSelection.exit().remove()
    actorNodeSelection.enter().call(createActorNode)
    actorNodeSelection.call(applyActorNode)

    setSvgHeight(height)
    setSvgWidth(width)
  }, [planNodeDependencyDag])

  return (
    <svg ref={svgRef} width={`${svgWidth}px`} height={`${svgHeight}px`}>
      <rect className="bounding-box" />
      <g className="links" />
      <g className="nodes" />
    </svg>
  )
}
