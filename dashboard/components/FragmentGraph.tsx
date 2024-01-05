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
  ActorBox,
  ActorBoxPosition,
  generateBoxLinks,
  layout,
} from "../lib/layout"
import { PlanNodeDatum } from "../pages/fragment_graph"
import BackPressureTable from "./BackPressureTable"

const ReactJson = loadable(() => import("react-json-view"))

interface Point {
  x: number
  y: number
}

interface ActorLayout {
  layoutRoot: d3.HierarchyPointNode<PlanNodeDatum>
  width: number
  height: number
  extraInfo: string
}

type PlanNodeDesc = ActorLayout & Point & { id: string }

type Enter<Type> = Type extends d3.Selection<any, infer B, infer C, infer D>
  ? d3.Selection<d3.EnterElement, B, C, D>
  : never

function treeLayoutFlip<Datum>(
  root: d3.HierarchyNode<Datum>,
  { dx, dy }: { dx: number; dy: number }
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
const actorMarginX = nodeRadius
const actorMarginY = nodeRadius
const actorDistanceX = nodeRadius * 5
const actorDistanceY = nodeRadius * 5

export default function FragmentGraph({
  planNodeDependencies,
  fragmentDependency,
  selectedFragmentId,
}: {
  planNodeDependencies: Map<string, d3.HierarchyNode<PlanNodeDatum>>
  fragmentDependency: ActorBox[]
  selectedFragmentId: string | undefined
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
    const layoutActorResult = new Map<string, ActorLayout>()
    const includedFragmentIds = new Set<string>()
    for (const [fragmentId, fragmentRoot] of deps) {
      const layoutRoot = treeLayoutFlip(fragmentRoot, {
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
      layoutActorResult.set(fragmentId, {
        layoutRoot,
        width,
        height,
        extraInfo: `Actor ${fragmentRoot.data.actor_ids?.join(", ")}` || "",
      })
      includedFragmentIds.add(fragmentId)
    }
    const fragmentLayout = layout(
      fragmentDependencyDag.map(({ width: _1, height: _2, id, ...data }) => {
        const { width, height } = layoutActorResult.get(id)!
        return { width, height, id, ...data }
      }),
      actorDistanceX,
      actorDistanceY
    )
    const fragmentLayoutPosition = new Map<string, { x: number; y: number }>()
    fragmentLayout.forEach(({ id, x, y }: ActorBoxPosition) => {
      fragmentLayoutPosition.set(id, { x, y })
    })
    const layoutResult: PlanNodeDesc[] = []
    for (const [fragmentId, result] of layoutActorResult) {
      const { x, y } = fragmentLayoutPosition.get(fragmentId)!
      layoutResult.push({ id: fragmentId, x, y, ...result })
    }
    let svgWidth = 0
    let svgHeight = 0
    layoutResult.forEach(({ x, y, width, height }) => {
      svgHeight = Math.max(svgHeight, y + height + 50)
      svgWidth = Math.max(svgWidth, x + width)
    })
    const links = generateBoxLinks(fragmentLayout)
    return {
      layoutResult,
      fragmentLayout,
      svgWidth,
      svgHeight,
      links,
      includedFragmentIds,
    }
  }, [planNodeDependencies, fragmentDependency])

  const {
    svgWidth,
    svgHeight,
    links,
    fragmentLayout: fragmentDependencyDag,
    layoutResult: planNodeDependencyDag,
    includedFragmentIds,
  } = planNodeDependencyDagCallback()

  useEffect(() => {
    if (planNodeDependencyDag) {
      const svgNode = svgRef.current
      const svgSelection = d3.select(svgNode)

      // How to draw edges
      const treeLink = d3
        .linkHorizontal<any, Point>()
        .x((d: Point) => d.x)
        .y((d: Point) => d.y)

      const isSelected = (id: string) => id === selectedFragmentId

      const applyFragment = (gSel: ActorSelection) => {
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
          .attr("dy", ({ height }) => height - actorMarginY + 12)
          .attr("dx", ({ width }) => width - actorMarginX)
          .attr("fill", "black")
          .attr("font-size", 12)

        // Fragment text line 2 (actor ids)
        let text2 = gSel.select<SVGTextElement>(".text-actor-id")
        if (text2.empty()) {
          text2 = gSel.append("text").attr("class", "text-actor-id")
        }

        text2
          .attr("fill", "black")
          .text(({ extraInfo }) => extraInfo)
          .attr("font-family", "inherit")
          .attr("text-anchor", "end")
          .attr("dy", ({ height }) => height - actorMarginY + 24)
          .attr("dx", ({ width }) => width - actorMarginX)
          .attr("fill", "black")
          .attr("font-size", 12)

        // Actor bounding box
        let boundingBox = gSel.select<SVGRectElement>(".bounding-box")
        if (boundingBox.empty()) {
          boundingBox = gSel.append("rect").attr("class", "bounding-box")
        }

        boundingBox
          .attr("width", ({ width }) => width - actorMarginX * 2)
          .attr("height", ({ height }) => height - actorMarginY * 2)
          .attr("x", actorMarginX)
          .attr("y", actorMarginY)
          .attr("fill", "white")
          .attr("stroke-width", ({ id }) => (isSelected(id) ? 3 : 1))
          .attr("rx", 5)
          .attr("stroke", ({ id }) =>
            isSelected(id) ? theme.colors.blue[500] : theme.colors.gray[500]
          )

        // Actor links
        let linkSelection = gSel.select<SVGGElement>(".links")
        if (linkSelection.empty()) {
          linkSelection = gSel.append("g").attr("class", "links")
        }

        const applyLink = (sel: LinkSelection) => sel.attr("d", treeLink)

        const createLink = (sel: Enter<LinkSelection>) => {
          sel
            .append("path")
            .attr("fill", "none")
            .attr("stroke", theme.colors.gray[700])
            .attr("stroke-width", 1.5)
            .call(applyLink)
          return sel
        }

        const links = linkSelection
          .selectAll<SVGPathElement, null>("path")
          .data(({ layoutRoot }) => layoutRoot.links())
        type LinkSelection = typeof links

        links.enter().call(createLink)
        links.call(applyLink)
        links.exit().remove()

        // Actor nodes
        let nodes = gSel.select<SVGGElement>(".nodes")
        if (nodes.empty()) {
          nodes = gSel.append("g").attr("class", "nodes")
        }

        const applyActorNode = (g: ActorNodeSelection) => {
          g.attr("transform", (d) => `translate(${d.x},${d.y})`)

          let circle = g.select<SVGCircleElement>("circle")
          if (circle.empty()) {
            circle = g.append("circle")
          }

          circle
            .attr("fill", theme.colors.blue[500])
            .attr("r", nodeRadius)
            .style("cursor", "pointer")
            .on("click", (_d, i) => openPlanNodeDetail(i.data))

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

          return g
        }

        const createActorNode = (sel: Enter<ActorNodeSelection>) =>
          sel.append("g").attr("class", "actor-node").call(applyActorNode)

        const actorNodeSelection = nodes
          .selectAll<SVGGElement, null>(".actor-node")
          .data(({ layoutRoot }) => layoutRoot.descendants())
        type ActorNodeSelection = typeof actorNodeSelection

        actorNodeSelection.exit().remove()
        actorNodeSelection.enter().call(createActorNode)
        actorNodeSelection.call(applyActorNode)
      }

      const createActor = (sel: Enter<ActorSelection>) => {
        const gSel = sel.append("g").attr("class", "actor").call(applyFragment)
        return gSel
      }

      const actorSelection = svgSelection
        .select<SVGGElement>(".actors")
        .selectAll<SVGGElement, null>(".actor")
        .data(planNodeDependencyDag)
      type ActorSelection = typeof actorSelection

      actorSelection.enter().call(createActor)
      actorSelection.call(applyFragment)
      actorSelection.exit().remove()

      const edgeSelection = svgSelection
        .select<SVGGElement>(".actor-links")
        .selectAll<SVGPathElement, null>(".actor-link")
        .data(links)
      type EdgeSelection = typeof edgeSelection

      const curveStyle = d3.curveMonotoneX

      const line = d3
        .line<{ x: number; y: number }>()
        .curve(curveStyle)
        .x(({ x }) => x)
        .y(({ y }) => y)

      const applyEdge = (sel: EdgeSelection) =>
        sel
          .attr("d", ({ points }) => line(points))
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
        sel.append("path").attr("class", "actor-link").call(applyEdge)

      edgeSelection.enter().call(createEdge)
      edgeSelection.call(applyEdge)
      edgeSelection.exit().remove()
    }
  }, [planNodeDependencyDag, links, selectedFragmentId, openPlanNodeDetail])

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
        <g className="actor-links" />
        <g className="actors" />
      </svg>
      <BackPressureTable selectedFragmentIds={includedFragmentIds} />
    </Fragment>
  )
}
