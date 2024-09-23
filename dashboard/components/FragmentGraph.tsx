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
import { cloneDeep } from "lodash"
import { Fragment, useCallback, useEffect, useRef, useState } from "react"
import {
  Edge,
  Enter,
  FragmentBox,
  FragmentBoxPosition,
  Position,
  generateFragmentEdges,
  layoutItem,
} from "../lib/layout"
import { PlanNodeDatum } from "../pages/fragment_graph"
import { StreamNode } from "../proto/gen/stream_plan"
import { backPressureColor, backPressureWidth } from "./utils/backPressure"

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
const fragmentDistanceX = nodeRadius * 2
const fragmentDistanceY = nodeRadius * 2

export default function FragmentGraph({
  planNodeDependencies,
  fragmentDependency,
  selectedFragmentId,
  backPressures,
}: {
  planNodeDependencies: Map<string, d3.HierarchyNode<PlanNodeDatum>>
  fragmentDependency: FragmentBox[]
  selectedFragmentId?: string
  backPressures?: Map<string, number>
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

    const layoutFragmentResult = new Map<string, any>()
    const includedFragmentIds = new Set<string>()
    for (const [fragmentId, fragmentRoot] of deps) {
      const layoutRoot = treeLayoutFlip(fragmentRoot, {
        dx: nodeMarginX,
        dy: nodeMarginY,
      })
      let { width, height } = boundBox(layoutRoot, {
        margin: {
          left: nodeRadius * 4 + fragmentMarginX,
          right: nodeRadius * 4 + fragmentMarginX,
          top: nodeRadius * 3 + fragmentMarginY,
          bottom: nodeRadius * 4 + fragmentMarginY,
        },
      })
      layoutFragmentResult.set(fragmentId, {
        layoutRoot,
        width,
        height,
        actorIds: fragmentRoot.data.actorIds ?? [],
      })
      includedFragmentIds.add(fragmentId)
    }

    const fragmentLayout = layoutItem(
      fragmentDependencyDag.map(({ width: _1, height: _2, id, ...data }) => {
        const { width, height } = layoutFragmentResult.get(id)!
        return { width, height, id, ...data }
      }),
      fragmentDistanceX,
      fragmentDistanceY
    )
    const fragmentLayoutPosition = new Map<string, Position>()
    fragmentLayout.forEach(({ id, x, y }: FragmentBoxPosition) => {
      fragmentLayoutPosition.set(id, { x, y })
    })

    const layoutResult: FragmentLayout[] = []
    for (const [fragmentId, result] of layoutFragmentResult) {
      const { x, y } = fragmentLayoutPosition.get(fragmentId)!
      layoutResult.push({ id: fragmentId, x, y, ...result })
    }

    let svgWidth = 0
    let svgHeight = 0
    layoutResult.forEach(({ x, y, width, height }) => {
      svgHeight = Math.max(svgHeight, y + height + 50)
      svgWidth = Math.max(svgWidth, x + width)
    })
    const edges = generateFragmentEdges(fragmentLayout)

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
          .attr("dy", ({ height }) => height - fragmentMarginY + 12)
          .attr("dx", ({ width }) => width - fragmentMarginX)
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
          .attr("dy", ({ height }) => height - fragmentMarginY + 24)
          .attr("dx", ({ width }) => width - fragmentMarginX)
          .attr("fill", "black")
          .attr("font-size", 12)

        // Fragment bounding box
        let boundingBox = gSel.select<SVGRectElement>(".bounding-box")
        if (boundingBox.empty()) {
          boundingBox = gSel.append("rect").attr("class", "bounding-box")
        }

        boundingBox
          .attr("width", ({ width }) => width - fragmentMarginX * 2)
          .attr("height", ({ height }) => height - fragmentMarginY * 2)
          .attr("x", fragmentMarginX)
          .attr("y", fragmentMarginY)
          .attr("fill", "white")
          .attr("stroke-width", ({ id }) => (isSelected(id) ? 3 : 1))
          .attr("rx", 5)
          .attr("stroke", ({ id }) =>
            isSelected(id) ? theme.colors.blue[500] : theme.colors.gray[500]
          )

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

        const isEdgeSelected = (d: Edge) =>
          isSelected(d.source) || isSelected(d.target)

        const color = (d: Edge) => {
          if (backPressures) {
            let value = backPressures.get(`${d.target}_${d.source}`)
            if (value) {
              return backPressureColor(value)
            }
          }

          return isEdgeSelected(d)
            ? theme.colors.blue["500"]
            : theme.colors.gray["300"]
        }

        const width = (d: Edge) => {
          if (backPressures) {
            let value = backPressures.get(`${d.target}_${d.source}`)
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
        sel.append("g").attr("class", "fragment-edge").call(applyEdge)

      edgeSelection.enter().call(createEdge)
      edgeSelection.call(applyEdge)
      edgeSelection.exit().remove()
    }
  }, [
    fragmentLayout,
    fragmentEdgeLayout,
    backPressures,
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
