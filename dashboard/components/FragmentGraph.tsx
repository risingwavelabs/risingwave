import {
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  theme,
  useDisclosure,
} from "@chakra-ui/react"
import loadable from "@loadable/component"
import * as d3 from "d3"
import * as dagre from "dagre"
import { cloneDeep } from "lodash"
import { Fragment, useCallback, useEffect, useRef, useState } from "react"
import { Edge, Enter, FragmentBox, Position } from "../lib/layout"
import { PlanNodeDatum } from "../pages/fragment_graph"
import { ChannelDeltaStats, FragmentStats } from "../proto/gen/monitor_service"
import { StreamNode } from "../proto/gen/stream_plan"
import {
  backPressureColor,
  backPressureWidth,
  epochToUnixMillis,
  latencyToColor,
} from "./utils/backPressure"

const ReactJson = loadable(() => import("react-json-view"))

type FragmentLayout = {
  id: string
  layoutRoot: d3.HierarchyPointNode<PlanNodeDatum>
  width: number
  height: number
  actorIds: string[]
} & Position

function treeLayoutFlip<Datum>(
  root: d3.HierarchyNode<Datum>,
  { dx, dy }: { dx: number; dy: number }
): d3.HierarchyPointNode<Datum> {
  const tree = d3.tree<Datum>().nodeSize([dy, dx])

  // Flip x, y
  const treeRoot = tree(root)

  // Flip back x, y
  treeRoot.each((d: Position) => ([d.x, d.y] = [d.y, d.x]))

  // LTR -> RTL
  treeRoot.each((d: Position) => (d.x = -d.x))

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
const fragmentMarginX = nodeRadius * 2
const fragmentMarginY = nodeRadius * 2
const fragmentDistanceX = nodeRadius * 5
const fragmentDistanceY = nodeRadius * 4

export default function FragmentGraph({
  planNodeDependencies,
  fragmentDependency,
  selectedFragmentId,
  channelStats,
  fragmentStats,
}: {
  planNodeDependencies: Map<string, d3.HierarchyNode<PlanNodeDatum>>
  fragmentDependency: FragmentBox[]
  selectedFragmentId?: string
  channelStats?: Map<string, ChannelDeltaStats>
  fragmentStats?: { [fragmentId: number]: FragmentStats }
}) {
  const svgRef = useRef<SVGSVGElement>(null)

  const { isOpen, onOpen, onClose } = useDisclosure()
  const [currentStreamNode, setCurrentStreamNode] = useState<PlanNodeDatum>()

  const openPlanNodeDetail = useCallback(
    (node: PlanNodeDatum) => {
      setCurrentStreamNode(node)
      onOpen()
    },
    [onOpen, setCurrentStreamNode]
  )

  const planNodeDependencyDagCallback = useCallback(() => {
    const deps = cloneDeep(planNodeDependencies)
    const fragmentDependencyDag = cloneDeep(fragmentDependency)

    // Layer 1: Keep existing d3-hierarchy layout for actors within fragments
    const layoutFragmentResult = new Map<string, FragmentLayout>()
    const includedFragmentIds = new Set<string>()
    for (const [fragmentId, fragmentRoot] of deps) {
      const layoutRoot = treeLayoutFlip(fragmentRoot, {
        dx: nodeMarginX,
        dy: nodeMarginY,
      })
      let { width, height } = boundBox(layoutRoot, {
        margin: {
          left: nodeRadius * 4,
          right: nodeRadius * 4,
          top: nodeRadius * 3,
          bottom: nodeRadius * 4,
        },
      })
      layoutFragmentResult.set(fragmentId, {
        layoutRoot,
        width,
        height,
        actorIds: fragmentRoot.data.actorIds ?? [],
      } as FragmentLayout)
      includedFragmentIds.add(fragmentId)
    }

    // Layer 2: Use dagre for fragment-level layout
    const g = new dagre.graphlib.Graph()

    // Configure the graph
    g.setGraph({
      rankdir: "LR",
      nodesep: fragmentDistanceY,
      ranksep: fragmentDistanceX,
      marginx: fragmentMarginX,
      marginy: fragmentMarginY,
    })

    // Default edge labels
    g.setDefaultEdgeLabel(() => ({}))

    // Add fragment nodes
    fragmentDependencyDag.forEach(({ id, parentIds }) => {
      const fragmentLayout = layoutFragmentResult.get(id)!
      g.setNode(id, fragmentLayout)
    })

    // Add fragment edges
    fragmentDependencyDag.forEach(({ id, parentIds }) => {
      parentIds?.forEach((parentId) => {
        g.setEdge(parentId, id)
      })
    })

    // Perform layout
    dagre.layout(g)

    // Convert to final format
    const layoutResult = g.nodes().map((id) => {
      const node = g.node(id) as FragmentLayout
      return {
        id,
        x: node.x - node.width / 2,
        y: node.y - node.height / 2,
        width: node.width,
        height: node.height,
        layoutRoot: node.layoutRoot,
        actorIds: node.actorIds,
      } as FragmentLayout
    })

    // Get edges with points
    const edges = g.edges().map((e) => {
      const edge = g.edge(e)
      return {
        source: e.v,
        target: e.w,
        points: edge.points || [],
      }
    })

    // Calculate overall SVG dimensions
    let svgWidth = 0
    let svgHeight = 0
    layoutResult.forEach(({ x, y, width, height }) => {
      svgWidth = Math.max(svgWidth, x + width + 50)
      svgHeight = Math.max(svgHeight, y + height + 50)
    })

    return {
      layoutResult,
      svgWidth,
      svgHeight,
      edges,
      includedFragmentIds,
    }
  }, [planNodeDependencies, fragmentDependency])

  const {
    svgWidth,
    svgHeight,
    edges: fragmentEdgeLayout,
    layoutResult: fragmentLayout,
    includedFragmentIds,
  } = planNodeDependencyDagCallback()

  useEffect(() => {
    if (fragmentLayout) {
      const now_ms = Date.now()
      const svgNode = svgRef.current
      const svgSelection = d3.select(svgNode)

      // How to draw edges
      const treeLink = d3
        .linkHorizontal<any, Position>()
        .x((d: Position) => d.x)
        .y((d: Position) => d.y)

      const isSelected = (id: string) => id === selectedFragmentId

      // Fragments
      const applyFragment = (gSel: FragmentSelection) => {
        gSel.attr("transform", ({ x, y }) => `translate(${x}, ${y})`)

        // Fragment text line 1 (fragment id)
        let text = gSel.select<SVGTextElement>(".text-frag-id")
        if (text.empty()) {
          text = gSel.append("text").attr("class", "text-frag-id")
        }

        text
          .attr("fill", "black")
          .text(({ id }) => `Fragment ${id}`)
          .attr("font-family", "inherit")
          .attr("text-anchor", "end")
          .attr("dy", ({ height }) => height + 12)
          .attr("dx", ({ width }) => width)
          .attr("fill", "black")
          .attr("font-size", 12)

        // Fragment text line 2 (actor ids)
        let text2 = gSel.select<SVGTextElement>(".text-actor-id")
        if (text2.empty()) {
          text2 = gSel.append("text").attr("class", "text-actor-id")
        }

        text2
          .attr("fill", "black")
          .text(({ actorIds }) => `Actor ${actorIds.join(", ")}`)
          .attr("font-family", "inherit")
          .attr("text-anchor", "end")
          .attr("dy", ({ height }) => height + 24)
          .attr("dx", ({ width }) => width)
          .attr("fill", "black")
          .attr("font-size", 12)

        // Fragment bounding box
        let boundingBox = gSel.select<SVGRectElement>(".bounding-box")
        if (boundingBox.empty()) {
          boundingBox = gSel.append("rect").attr("class", "bounding-box")
        }

        boundingBox
          .attr("width", ({ width }) => width)
          .attr("height", ({ height }) => height)
          .attr("x", 0)
          .attr("y", 0)
          .attr(
            "fill",
            fragmentStats
              ? ({ id }) => {
                  const fragmentId = parseInt(id)
                  if (isNaN(fragmentId) || !fragmentStats[fragmentId]) {
                    return "white"
                  }
                  let currentMs = epochToUnixMillis(
                    fragmentStats[fragmentId].currentEpoch
                  )
                  return latencyToColor(now_ms - currentMs, "white")
                }
              : "white"
          )
          .attr("stroke-width", ({ id }) => (isSelected(id) ? 3 : 1))
          .attr("rx", 5)
          .attr("stroke", ({ id }) =>
            isSelected(id) ? theme.colors.blue[500] : theme.colors.gray[500]
          )

        const getTooltipContent = (id: string) => {
          const fragmentId = parseInt(id)
          const stats = fragmentStats?.[fragmentId]
          const latencySeconds = stats
            ? ((now_ms - epochToUnixMillis(stats.currentEpoch)) / 1000).toFixed(
                2
              )
            : "N/A"
          const epoch = stats?.currentEpoch ?? "N/A"

          return `<b>Fragment ${fragmentId}</b><br>Epoch: ${epoch}<br>Latency: ${latencySeconds} seconds`
        }

        boundingBox
          .on("mouseover", (event, { id }) => {
            // Remove existing tooltip if any
            d3.selectAll(".tooltip").remove()

            // Create new tooltip
            d3.select("body")
              .append("div")
              .attr("class", "tooltip")
              .style("position", "absolute")
              .style("background", "white")
              .style("padding", "10px")
              .style("border", "1px solid #ddd")
              .style("border-radius", "4px")
              .style("pointer-events", "none")
              .style("left", event.pageX + 10 + "px")
              .style("top", event.pageY + 10 + "px")
              .style("font-size", "12px")
              .html(getTooltipContent(id))
          })
          .on("mousemove", (event) => {
            d3.select(".tooltip")
              .style("left", event.pageX + 10 + "px")
              .style("top", event.pageY + 10 + "px")
          })
          .on("mouseout", () => {
            d3.selectAll(".tooltip").remove()
          })

        // Stream node edges
        let edgeSelection = gSel.select<SVGGElement>(".edges")
        if (edgeSelection.empty()) {
          edgeSelection = gSel.append("g").attr("class", "edges")
        }

        const applyEdge = (sel: EdgeSelection) => sel.attr("d", treeLink)

        const createEdge = (sel: Enter<EdgeSelection>) => {
          sel
            .append("path")
            .attr("fill", "none")
            .attr("stroke", theme.colors.gray[700])
            .attr("stroke-width", 1.5)
            .call(applyEdge)
          return sel
        }

        const edges = edgeSelection
          .selectAll<SVGPathElement, null>("path")
          .data(({ layoutRoot }) => layoutRoot.links())
        type EdgeSelection = typeof edges

        edges.enter().call(createEdge)
        edges.call(applyEdge)
        edges.exit().remove()

        // Stream nodes in fragment
        let nodes = gSel.select<SVGGElement>(".nodes")
        if (nodes.empty()) {
          nodes = gSel.append("g").attr("class", "nodes")
        }

        const applyStreamNode = (g: StreamNodeSelection) => {
          g.attr("transform", (d) => `translate(${d.x},${d.y})`)

          // Node circle
          let circle = g.select<SVGCircleElement>("circle")
          if (circle.empty()) {
            circle = g.append("circle")
          }

          circle
            .attr("fill", theme.colors.blue[500])
            .attr("r", nodeRadius)
            .style("cursor", "pointer")
            .on("click", (_d, i) => openPlanNodeDetail(i.data))

          // Node name under the circle
          let text = g.select<SVGTextElement>("text")
          if (text.empty()) {
            text = g.append("text")
          }

          text
            .attr("fill", "black")
            .text((d) => d.data.name)
            .attr("font-family", "inherit")
            .attr("text-anchor", "middle")
            .attr("dy", nodeRadius * 1.8)
            .attr("fill", "black")
            .attr("font-size", 12)
            .attr("transform", "rotate(-8)")

          // Node tooltip
          let title = g.select<SVGTitleElement>("title")
          if (title.empty()) {
            title = g.append<SVGTitleElement>("title")
          }

          title.text((d) => (d.data.node as StreamNode).identity ?? d.data.name)

          return g
        }

        const createStreamNode = (sel: Enter<StreamNodeSelection>) =>
          sel.append("g").attr("class", "stream-node").call(applyStreamNode)

        const streamNodeSelection = nodes
          .selectAll<SVGGElement, null>(".stream-node")
          .data(({ layoutRoot }) => layoutRoot.descendants())
        type StreamNodeSelection = typeof streamNodeSelection

        streamNodeSelection.exit().remove()
        streamNodeSelection.enter().call(createStreamNode)
        streamNodeSelection.call(applyStreamNode)
      }

      const createFragment = (sel: Enter<FragmentSelection>) =>
        sel.append("g").attr("class", "fragment").call(applyFragment)

      const fragmentSelection = svgSelection
        .select<SVGGElement>(".fragments")
        .selectAll<SVGGElement, null>(".fragment")
        .data(fragmentLayout)
      type FragmentSelection = typeof fragmentSelection

      fragmentSelection.enter().call(createFragment)
      fragmentSelection.call(applyFragment)
      fragmentSelection.exit().remove()

      // Fragment Edges
      const edgeSelection = svgSelection
        .select<SVGGElement>(".fragment-edges")
        .selectAll<SVGGElement, null>(".fragment-edge")
        .data(fragmentEdgeLayout)
      type EdgeSelection = typeof edgeSelection

      const curveStyle = d3.curveBasis

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

        const isEdgeSelected = (d: Edge) =>
          isSelected(d.source) || isSelected(d.target)

        const color = (d: Edge) => {
          if (channelStats) {
            let value = channelStats.get(
              `${d.source}_${d.target}`
            )?.backpressureRate
            if (value) {
              return backPressureColor(value)
            }
          }

          return isEdgeSelected(d)
            ? theme.colors.blue["500"]
            : theme.colors.gray["300"]
        }

        const width = (d: Edge) => {
          if (channelStats) {
            let value = channelStats.get(
              `${d.source}_${d.target}`
            )?.backpressureRate
            if (value) {
              return backPressureWidth(value, 30)
            }
          }

          return isEdgeSelected(d) ? 4 : 2
        }

        path
          .attr("d", ({ points }) => line(points))
          .attr("fill", "none")
          .attr("stroke-width", width)
          .attr("stroke", color)

        path
          .on("mouseover", (event, d) => {
            // Remove existing tooltip if any
            d3.selectAll(".tooltip").remove()

            // Create new tooltip
            const stats = channelStats?.get(`${d.source}_${d.target}`)
            const tooltipText = `<b>Fragment ${d.source} → ${
              d.target
            }</b><br>Backpressure: ${
              stats?.backpressureRate != null
                ? `${(stats.backpressureRate * 100).toFixed(2)}%`
                : "N/A"
            }<br>Recv Throughput: ${
              stats?.recvThroughput != null
                ? `${stats.recvThroughput.toFixed(2)} rows/s`
                : "N/A"
            }<br>Send Throughput: ${
              stats?.sendThroughput != null
                ? `${stats.sendThroughput.toFixed(2)} rows/s`
                : "N/A"
            }`
            d3.select("body")
              .append("div")
              .attr("class", "tooltip")
              .style("position", "absolute")
              .style("background", "white")
              .style("padding", "10px")
              .style("border", "1px solid #ddd")
              .style("border-radius", "4px")
              .style("pointer-events", "none")
              .style("left", event.pageX + 10 + "px")
              .style("top", event.pageY + 10 + "px")
              .style("font-size", "12px")
              .html(tooltipText)
          })
          .on("mousemove", (event) => {
            d3.select(".tooltip")
              .style("left", event.pageX + 10 + "px")
              .style("top", event.pageY + 10 + "px")
          })
          .on("mouseout", () => {
            d3.selectAll(".tooltip").remove()
          })

        return gSel
      }
      const createEdge = (sel: Enter<EdgeSelection>) =>
        sel.append("g").attr("class", "fragment-edge").call(applyEdge)

      edgeSelection.enter().call(createEdge)
      edgeSelection.call(applyEdge)
      edgeSelection.exit().remove()
    }
  }, [
    fragmentLayout,
    fragmentEdgeLayout,
    channelStats,
    fragmentStats,
    selectedFragmentId,
    openPlanNodeDetail,
  ])

  return (
    <Fragment>
      <Modal isOpen={isOpen} onClose={onClose} size="5xl">
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>
            {currentStreamNode?.operatorId} - {currentStreamNode?.name}
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            {isOpen && currentStreamNode?.node && (
              <ReactJson
                shouldCollapse={({ name }) =>
                  name === "input" || name === "fields" || name === "streamKey"
                } // collapse top-level fields for better readability
                src={currentStreamNode.node}
                collapsed={3}
                name={null}
                displayDataTypes={false}
              />
            )}
          </ModalBody>

          <ModalFooter>
            <Button colorScheme="blue" mr={3} onClick={onClose}>
              Close
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
      <svg ref={svgRef} width={`${svgWidth}px`} height={`${svgHeight}px`}>
        <g className="fragment-edges" />
        <g className="fragments" />
      </svg>
    </Fragment>
  )
}
