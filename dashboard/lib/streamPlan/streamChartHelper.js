import * as d3 from "d3";
import * as color from "../color";
import { getConnectedComponent, treeBfs } from "../algo";
import { cloneDeep, max } from "lodash";
import { newNumberArray } from "../util";
import StreamPlanParser from "./parser";
// Actor constant
// 
// =======================================================
//                  ^ 
//                  | actorBoxPadding
//                  v
//        --┌───────────┐      
//        | │      node │                                          
//        | │<--->radius│>───────\        
//        | │           │        │        
//        | └───────────┘        │
//        |                      │
//        | ┌───────────┐        │         ┌───────────┐
//        | │      node │        │         │      node │
// widthUnit│<--->radius│>───────┼────────>│<--->radius│
//        | │           │        │         │           │
//        | └───────────┘        │         └───────────┘
//        |                      │
//        | ┌───────────┐        │
//        | │      node │        │      
//        | │<--->radius│>───────/
//        | │           │                      
//       ---└───────────┘
//          |-----------------heightUnit---------------|
//
const nodeRadius = 30; // the radius of the tree nodes in an actor
const nodeStrokeWidth = 5; // the stroke width of the link of the tree nodes in an actor
const widthUnit = 230; // the width of a tree node in an actor
const heightUnit = 250; // the height of a tree layer in an actor
const actorBoxPadding = 100; // box padding
const actorBoxStroke = 15; // the width of the stroke of the box
const linkStrokeWidth = 30; // the width of the link between nodes
const linkFlowEffectDuration = 1000; // the duration of the flow effect amination
const linkStrokeDash = "10, 10";

// Stream Plan constant
const gapBetweenRow = 100;
const gapBetweenLayer = 300;
const gapBetweenFlowChart = 500;

// Draw linking effect
const DrawLinkEffect = true;
const bendGap = 50; // try example at: http://bl.ocks.org/d3indepth/b6d4845973089bc1012dec1674d3aff8
const connectionGap = 50;

// font constant
const fontSize = 30;

// color
const operatorColor = (actor, operator) => {
  if (operator.dispatcherType) {
    return "#fff";
  }
  return color.TwoGradient(actor.actorId)[0];
}
const actorBoxStrokeColor = (actor) => {
  return color.TwoGradient(actor.actorId)[0];
}
const actorBoxBackgroundColor = (actor) => {
  return color.TwoGradient(actor.actorId)[1];
}
const actorOutgoinglinkColor = (actor) => {
  return color.TwoGradient(actor.actorId)[0];
}



/**
 * Construct an id for a link in actor box.
 * You may use this method to query and get the svg element
 * of the link.
 * @param {{id: number}} node1 a node (operator) in an actor box
 * @param {{id: number}} node2 a node (operator) in an actor box
 * @returns {string} The link id
 */
function constructNodeLinkId(node1, node2) {
  return "node-" + (node1.id > node2.id ? node1.id + "-" + node2.id : node2.id + "-" + node1.id);
}

/**
 * Construct an id for a node (operator) in an actor box.
 * You may use this method to query and get the svg element
 * of the link.
 * @param {{id: number}} node a node (operator) in an actor box 
 * @returns {string} The node id
 */
function constructNodeId(node) {
  return "node-" + node.id;
}


/**
 * Work flow
 *   1. Get the layout for actor boxes (Calculate the base coordination of each actor box)
 *   2. Get the layout for operators in each actor box
 *   3. Draw all actor boxes
 *   4. Draw link between actor boxes
 * 
 * 
 * Dependencies
 *   layoutActorBox         <- dagLayout         <- drawActorBox      <- drawFlow 
 *   [ The layout of the ]     [ The layout of ]    [ Draw an actor ]    [ Draw many actors   ]
 *   [ operators in an   ]     [ actors in a   ]    [ in specified  ]    [ and links between  ]
 *   [ actor.            ]     [ stram plan    ]    [ place         ]    [ them.              ]
 *
 */
export class StreamChartHelper {

  /**
   * 
   * @param {d3.Selection} g The svg group to contain the graph 
   * @param {*} actorProto The raw response from the meta node
   * @param {(e, node) => void} onNodeClick The callback function trigged when a node is click
   */
  constructor(g, actorProto, onNodeClick) {
    this.topGroup = g;
    this.actorProto = actorProto;
    this.onNodeClick = onNodeClick;
  }

  //
  // A simple DAG layout algorithm.
  // The layout is built based on two rules.
  // 1. The link should have at two turnning points.
  // 2. The turnning point of a link should be placed
  //    at the margin after the layer of its starting point.
  // -------------------------------------------------------
  // Example 1: (X)-(Z) and (Y)-(Z) is valid.
  // Row 0   (X)---------------->(Z)
  //                         |
  // Row 1                   |
  //                         |
  // Row 2             (Y)---/
  //       Layer 1 | Layer 2 | Layer 3 
  // -------------------------------------------------------
  // Example 2: (A)-(B) is not valid.
  // Row 0   (X)   /---------\   (Z)
  //               |         |
  // Row 1   (A)---/   (Y)   |-->(B)
  //       
  //       Layer 1 | Layer 2 | Layer 3 
  // -------------------------------------------------------
  // Example 3: (C)-(Z) is not valid
  // Row 0   (X)             /-->(Z)
  //                         |
  // Row 1   (C)-------------/
  //       
  //        Layer 1 | Layer 2 | Layer 3  
  // -------------------------------------------------------
  // Note that the layer of each node can be different
  // For example:
  // Row 0   ( 1)     ( 3)      ( 5)      ( 2)      ( 9)
  // Row 1   ( 4)                         ( 6)      (10)
  // Row 2                                ( 7)      ( 8)
  //       Layer 0 | Layer 1 | Layer 2 | Layer 3 | Layer 4 |
  //
  // Row 0   ( 1)     ( 3)      ( 5)      ( 2)      ( 9)
  // Row 1            ( 4)                ( 6)      (10)
  // Row 2                                ( 7)      ( 8)
  //       Layer 0 | Layer 1 | Layer 2 | Layer 3 | Layer 4 |
  /**
   * Topological sort
   * @param {Array<Node>} nodes An array of node: {nextNodes: [...]} 
   * @returns {Map<Node, [number, number]>} position of each node
   */
  dagLayout(nodes) {
    let sorted = [];
    let _nodes = [];
    let node2dagNode = new Map();
    const visit = (n) => {
      if (n.temp) {
        throw Error("This is not a DAG");
      }
      if (!n.perm) {
        n.temp = true;
        let maxG = -1;
        for (let nextNode of n.node.nextNodes) {
          node2dagNode.get(nextNode).isInput = false;
          n.isOutput = false;
          let g = visit(node2dagNode.get(nextNode));
          if (g > maxG) {
            maxG = g;
          }
        }
        n.temp = false;
        n.perm = true;
        n.g = maxG + 1;
        sorted.unshift(n.node);
      }
      return n.g;
    }
    for (let node of nodes) {
      let dagNode = { node: node, temp: false, perm: false, isInput: true, isOutput: true };
      node2dagNode.set(node, dagNode);
      _nodes.push(dagNode);
    }
    let maxLayer = 0;
    for (let node of _nodes) {
      let g = visit(node);
      if (g > maxLayer) {
        maxLayer = g;
      }
    }
    // use the bottom up strategy to construct generation number 
    // makes the generation number of root node the samllest
    // to make the computation easier, need to flip it back.
    for (let node of _nodes) {
      // node.g = node.isInput ? 0 : (maxLayer - node.g); // TODO: determine which is more suitable
      node.g = maxLayer - node.g;
    }

    let layers = [];
    for (let i = 0; i < maxLayer + 1; ++i) {
      layers.push({
        nodes: [],
        occupyRow: new Set()
      });
    }
    let node2Layer = new Map();
    let node2Row = new Map();
    for (let node of _nodes) {
      layers[node.g].nodes.push(node.node);
      node2Layer.set(node.node, node.g);
    }

    // layers to rtn
    let rtn = new Map();

    const putNodeInPosition = (node, row) => {
      node2Row.set(node, row);
      layers[node2Layer.get(node)].occupyRow.add(row);
    }

    const occupyLine = (ls, le, r) => { // layer start, layer end, row
      for (let i = ls; i <= le; ++i) {
        layers[i].occupyRow.add(r);
      }
    }

    const hasOccupied = (layer, row) => layers[layer].occupyRow.has(row);

    const isStraightLineOccupied = (ls, le, r) => { // layer start, layer end, row
      if (r < 0) {
        return false;
      }
      for (let i = ls; i <= le; ++i) {
        if (hasOccupied(i, r)) {
          return true;
        }
      }
      return false;
    }

    for (let node of nodes) {
      node.nextNodes.sort((a, b) => node2Layer.get(b) - node2Layer.get(a));
    }

    for (let layer of layers) {
      for (let node of layer.nodes) {
        if (!node2Row.has(node)) { // checking node is not placed.
          for (let nextNode of node.nextNodes) {
            if (node2Row.has(nextNode)) {
              continue;
            }
            let r = -1;
            while (isStraightLineOccupied(node2Layer.get(node), node2Layer.get(nextNode), ++r)) {
              ;
            }
            putNodeInPosition(node, r);
            putNodeInPosition(nextNode, r);
            occupyLine(node2Layer.get(node) + 1, node2Layer.get(nextNode) - 1, r);
            break;
          }
          if (!node2Row.has(node)) {
            let r = -1;
            while (hasOccupied(node2Layer.get(node), ++r)) {
              ;
            }
            putNodeInPosition(node, r);
          }
        }
        // checking node is placed in some position
        for (let nextNode of node.nextNodes) {
          if (node2Row.has(nextNode)) {
            continue;
          }
          // check straight line position first
          let r = node2Row.get(node);
          if (!isStraightLineOccupied(node2Layer.get(node) + 1, node2Layer.get(nextNode), r)) {
            putNodeInPosition(nextNode, r);
            occupyLine(node2Layer.get(node) + 1, node2Layer.get(nextNode) - 1, r);
            continue;
          }
          // check lowest available position
          r = -1;
          while (isStraightLineOccupied(node2Layer.get(node) + 1, node2Layer.get(nextNode), ++r)) {
            ;
          }
          putNodeInPosition(nextNode, r);
          occupyLine(node2Layer.get(node) + 1, node2Layer.get(nextNode) - 1, r);
        }
      }
    }
    for (let node of nodes) {
      rtn.set(node.id, [node2Layer.get(node), node2Row.get(node)]);
    }

    return rtn;
  }

  /**
   * Calculate the position of each node in the actor box.
   * @param {{id: any, nextNodes: [], x: number, y: number}} rootNode The root node of an actor box (dispatcher)
   * @returns {[width, height]} The size of the actor box
   */
  calculateActorBoxSize(rootNode) {
    let rootNodeCopy = cloneDeep(rootNode);
    return this.layoutActorBox(rootNodeCopy, 0, 0);
  }

  /**
   * Calculate the position of each node (operator) in the actor box.
   * This will change the node's position
   * @param {{id: any, nextNodes: [], x: number, y: number}} rootNode The root node of an actor box (dispatcher)
   * @param {number} baseX The x coordination of the top-left corner of the actor box
   * @param {number} baseY The y coordination of the top-left corner of the actor box
   * @returns {[width, height]} The size of the actor box
   */
  layoutActorBox(rootNode, baseX, baseY) {
    // calculate nodes' required width
    let maxLayer = 0;
    const getRequiredWidth = (node, layer) => {
      if (node.width !== undefined) {
        return node.width;
      }

      if (layer > maxLayer) {
        maxLayer = layer;
      }

      node.layer = layer;

      let requiredWidth = 0;
      for (let nextNode of node.nextNodes) {
        requiredWidth += getRequiredWidth(nextNode, layer + 1);
      }

      node.isLeaf = requiredWidth === 0;

      node.width = requiredWidth > 0 ? requiredWidth : widthUnit;

      return node.width;
    }

    getRequiredWidth(rootNode, 0);

    // calculate nodes' position
    rootNode.x = baseX || 0;
    rootNode.y = baseY || 0;
    let leafY = rootNode.x - heightUnit * maxLayer;
    treeBfs(rootNode, (c) => {
      let tmpY = c.y - c.width / 2;
      for (let nextNode of c.nextNodes) {
        nextNode.x = nextNode.isLeaf ? leafY : c.x - heightUnit;
        nextNode.y = tmpY + nextNode.width / 2;
        tmpY += nextNode.width;
      }
    })

    // calculate box size
    let minX = Infinity;
    let maxX = -Infinity;
    let minY = Infinity;
    let maxY = -Infinity;
    treeBfs(rootNode, (node) => {
      if (node.x > maxX) {
        maxX = node.x;
      }
      if (node.x < minX) {
        minX = node.x;
      }
      if (node.y > maxY) {
        maxY = node.y;
      }
      if (node.y < minY) {
        minY = node.y;
      }
    });
    let boxWidth = maxX - minX;
    let boxHeight = maxY - minY;
    return [boxWidth + actorBoxPadding * 2, boxHeight + actorBoxPadding * 2];
  }

  /**
   * @param {{
   *   g: d3.Selection, 
   *   rootNode: {id: any, nextNodes: []}, 
   *   nodeColor: string, 
   *   strokeColor?: string,
   *   onNodeClicked?: (event, node) => void,
   *   onMouseOver?: (event, node) => void,
   *   onMouseOut?: (event, node) => void,
   *   baseX?: number,
   *   baseY?: number
   * }} props
   * @param {d3.Selection} props.g The target group contains this tree.
   * @param {{id: any, nextNodes: []}} props.rootNode The root node of the tree in the actor
   * @param {string} props.nodeColor [optinal] The filled color of nodes.
   * @param {string} props.strokeColor [optinal] The color of the stroke.
   * @param {(event, node) => void} props.onNodeClicked [optinal] The callback function when a node is clicked.
   * @param {(event, node) => void} props.onMouseOver [optinal] The callback function when the mouse enters a node.
   * @param {(event, node) => void} props.onMouseOut [optinal] The callback function when the mouse leaves a node.
   * @param {number} props.baseX [optinal] The x coordination of the lef-top corner. default: 0
   * @param {number} props.baseY [optinal] The y coordination of the lef-top corner. default: 0
   * @returns {d3.Selection} The group element of this tree
   */
  drawActorBox(props) {

    if (props.g === undefined) {
      throw Error("Invalid Argument: Target group cannot be undefined.");
    }

    const actor = props.actor;
    const group = props.g.append("g");
    const rootNode = props.rootNode || [];
    const baseX = props.x === undefined ? 0 : props.x;
    const baseY = props.y === undefined ? 0 : props.y;
    const strokeColor = props.strokeColor || "white";
    const linkColor = props.linkColor || "gray";

    const [boxWidth, boxHeight] = this.calculateActorBoxSize(rootNode);
    this.layoutActorBox(rootNode, baseX + boxWidth - actorBoxPadding, baseY + boxHeight / 2);

    const onNodeClicked = (e, node) => {
      this.onNodeClick && this.onNodeClick(e, node);
      props.onNodeClicked && props.onNodeClicked(e, node);
    }

    const onMouseOut = (e, node) => {
      props.onMouseOut && props.onMouseOut(e, node);
    }

    const onMouseOver = (e, node) => {
      props.onMouseOut && props.onMouseOver(e, node);
    }

    // draw box
    group.attr("id", "actor-" + actor.actorId);
    group.append("rect")
      .attr("width", boxWidth)
      .attr("height", boxHeight)
      .attr("x", baseX)
      .attr("y", baseY)
      .attr("fill", actorBoxBackgroundColor(actor))
      .attr("rx", 20)
      .attr("stroke-width", actorBoxStroke)
      .attr("stroke", actorBoxStrokeColor(actor))
    group.append("text")
      .attr("x", baseX)
      .attr("y", baseY - actorBoxStroke)
      .attr("font-size", fontSize)
      .text(`Fragment ${actor.fragmentId}, Actor ${actor.representedActorList}`);

    // draw links
    const linkData = [];
    treeBfs(rootNode, (c) => {
      for (let nextNode of c.nextNodes) {
        linkData.push({ sourceNode: c, nextNode: nextNode, source: [c.x, c.y], target: [nextNode.x, nextNode.y] });
      }
    });
    const linkGen = d3.linkHorizontal();
    group.selectAll("path")
      .data(linkData)
      .join("path")
      .attr("d", linkGen)
      .attr("fill", "none")
      .attr("id", (link) => constructNodeLinkId(link.sourceNode, link.nextNode))
      .style("stroke-width", linkStrokeWidth)
      .attr("stroke", linkColor);

    // draw nodes
    treeBfs(rootNode, (node) => {
      node.d3Selection = group.append("circle")
        .attr("r", nodeRadius)
        .attr("cx", node.x)
        .attr("cy", node.y)
        .attr("id", constructNodeId(node))
        .attr('stroke', strokeColor)
        .attr('fill', operatorColor(actor, node))
        .style('cursor', 'pointer')
        .style('stroke-width', nodeStrokeWidth)
        .on("click", (e) => onNodeClicked(e, node))
        .on("mouseover", (e) => onMouseOver(e, node))
        .on("mouseout", (e) => onMouseOut(e, node))
      group.append("text")
        .attr("x", node.x)
        .attr("y", node.y + nodeRadius + fontSize + 10)
        .attr("text-anchor", "middle")
        .attr("font-size", fontSize)
        .text(node.type ? node.type : node.dispatcherType);
    })

    // dataflow effect
    group.selectAll("path")
      .attr("stroke-dasharray", linkStrokeDash);
    if (DrawLinkEffect) {
      function repeat() {
        group.selectAll("path")
          .attr("stroke-dashoffset", "0")
          .transition()
          .duration(linkFlowEffectDuration / 2)
          .ease(d3.easeLinear)
          .attr("stroke-dashoffset", "20")
          .transition()
          .duration(linkFlowEffectDuration / 2)
          .ease(d3.easeLinear)
          .attr("stroke-dashoffset", "40")
          .on("end", repeat);
      }
      repeat();
    }

    // ~ END OF dataflow effect 

    return {
      g: group,
      x: baseX - boxWidth - actorBoxPadding,
      y: baseY - boxHeight / 2 - actorBoxPadding,
      width: boxWidth + actorBoxPadding * 2,
      height: boxHeight + actorBoxPadding * 2
    };
  }
  /**
   * 
   * @param {{
   *   g: d3.Selection, 
   *   actorDagList: Array, 
   *   baseX?: number, 
   *   baseY?: number
   * }} props
   * @param {d3.Selection} props.g The target group contains this group.
   * @param {Arrary} props.actorDagList A list of dag nodes constructed from actors
   * { id: actor.actorId, nextNodes: [], actor: actor }
   * @param {number} props.baseX [optional] The x coordination of left-top corner. default: 0.
   * @param {number} props.baseY [optional] The y coordination of left-top corner. default: 0.
   * @returns {{group: d3.Selection, width: number, height: number}} The size of the flow
   */
  drawFlow(props) {

    if (props.g === undefined) {
      throw Error("Invalid Argument: Target group cannot be undefined.");
    }

    const g = props.g;
    const actorDagList = props.actorDagList || [];
    const baseX = props.baseX || 0;
    const baseY = props.baseY || 0;

    let layoutPositionMapper = this.dagLayout(actorDagList);
    const actors = [];
    for (let actorDag of actorDagList) {
      actors.push(actorDag.actor);
    }

    // calculate actor box size
    for (let actor of actors) {
      [actor.boxWidth, actor.boxHeight] = this.calculateActorBoxSize(actor.rootNode);
      [actor.layer, actor.row] = layoutPositionMapper.get(actor.actorId);
    }

    // calculate the minimum required width of each layer and row
    let maxRow = 0;
    let maxLayer = 0;
    for (let actor of actors) {
      maxLayer = max([actor.layer, maxLayer]);
      maxRow = max([actor.row, maxRow]);
    }
    let rowGap = newNumberArray(maxRow + 1);
    let layerGap = newNumberArray(maxLayer + 1);
    for (let actor of actors) {
      layerGap[actor.layer] = max([layerGap[actor.layer], actor.boxWidth]);
      rowGap[actor.row] = max([rowGap[actor.row], actor.boxHeight]);
    }
    let row2y = newNumberArray(maxRow + 1);
    let layer2x = newNumberArray(maxLayer + 1);
    row2y = row2y.map((_, r) => {
      if (r === 0) {
        return 0;
      }
      let rtn = 0;
      for (let i = 0; i < r; ++i) {
        rtn += rowGap[i] + gapBetweenRow;
      }
      return rtn;
    })
    layer2x = layer2x.map((_, l) => {
      if (l === 0) {
        return 0;
      }
      let rtn = 0;
      for (let i = 0; i < l; ++i) {
        rtn += layerGap[i] + gapBetweenLayer;
      }
      return rtn;
    })


    // Draw fragment (represent by one actor)
    const group = g.append("g");
    const linkLayerBackground = group.append("g");
    const linkLayer = group.append("g");
    const fragmentLayer = group.append("g");
    linkLayerBackground.attr("class", "linkLayerBackground");
    linkLayer.attr("class", "linkLayer");
    fragmentLayer.attr("class", "fragmentLayer");

    let actorBoxList = [];
    for (let actor of actors) {
      let actorBox = this.drawActorBox({
        actor: actor,
        g: fragmentLayer,
        rootNode: actor.rootNode,
        x: baseX + layer2x[actor.layer],
        y: baseY + row2y[actor.row],
        strokeColor: "white",
        linkColor: "white",
      })
      actorBoxList.push(actorBox);
    }

    // Draw link between (represent by one actor)
    const getLinkBetweenPathStr = (start, end, compensation) => {
      const lineGen = d3.line().curve(d3.curveBasis);
      let pathStr = lineGen([
        start,
        [start[0] + compensation + actorBoxPadding + connectionGap, start[1]],
        [start[0] + compensation + actorBoxPadding + connectionGap + bendGap, start[1]],
        [start[0] + compensation + actorBoxPadding + connectionGap + bendGap, end[1]],
        [start[0] + compensation + actorBoxPadding + connectionGap + bendGap * 2, end[1]],
        end
      ]);
      return pathStr;
    }
    let linkData = [];
    for (let actor of actors) {
      for (let outputNode of actor.output) {
        linkData.push(
          {
            actor: actor,
            d: getLinkBetweenPathStr(
              [actor.rootNode.x, actor.rootNode.y],
              [outputNode.x, outputNode.y],
              layerGap[actor.layer] - actor.boxWidth
            )
          }
        )
      }
    }
    linkLayerBackground
      .selectAll("path")
      .data(linkData)
      .join("path")
      .attr("d", s => s.d)
      .attr("fill", "none")
      .style("stroke-width", 40)
      .attr("stroke", "#eee");
    linkLayer
      .selectAll("path")
      .data(linkData)
      .join("path")
      .attr("d", s => s.d)
      .attr("fill", "none")
      .style("stroke-width", 20)
      .attr("stroke", s => actorOutgoinglinkColor(s.actor));
    // dataflow effect
    linkLayer.selectAll("path")
      .attr("stroke-dasharray", "20, 20");
    if (DrawLinkEffect) {
      function repeat() {
        linkLayer.selectAll("path")
          .attr("stroke-dashoffset", "40")
          .transition()
          .duration(linkFlowEffectDuration / 2)
          .ease(d3.easeLinear)
          .attr("stroke-dashoffset", "20")
          .transition()
          .duration(linkFlowEffectDuration / 2)
          .ease(d3.easeLinear)
          .attr("stroke-dashoffset", "0")
          .on("end", repeat);
      }
      repeat();
    }
    // ~ END OF dataflow effect 

    // calculate box size
    let width = 0;
    let height = 0;
    for (let actorBox of actorBoxList) {

      let biggestX = actorBox.x - baseX + actorBox.width;
      let biggestY = actorBox.y - baseY + actorBox.height;
      width = max([biggestX, width]);
      height = max([biggestY, height]);
    }

    group.attr("class", "flowchart");
    return {
      g: group,
      width: width,
      height: height
    };
  }

  /**
   * A flow is an extracted connected component of actors of
   * the raw response from the meta node. This method will first
   * merge actors in the same fragment using some identifier 
   * (currently it is the id of the operator before the dispatcher).
   * And then use `drawFlow()` to draw each connected component.
   */
  drawManyFlow() {
    const g = this.topGroup;
    const actorProto = this.actorProto;
    const baseX = 0;
    const baseY = 0;

    g.attr("id", "");

    // remove actors in the same fragment TODO: show these actors in the graph
    const streamPlan = new StreamPlanParser(actorProto);
    const allActors = streamPlan.getParsedActorList();
    const dispatcherNode2ActorId = new Map();
    const fragmentRepresentedActors = [];
    for (let actor of allActors) {
      if (!dispatcherNode2ActorId.has(actor.fragmentId)) {
        fragmentRepresentedActors.push(actor);
        dispatcherNode2ActorId.set(actor.fragmentId, [actor.actorId]);
      } else {
        dispatcherNode2ActorId.get(actor.fragmentId).push(actor.actorId);
      }
    }
    for (let actor of fragmentRepresentedActors) {
      actor.representedActorList = dispatcherNode2ActorId.get(actor.fragmentId);
    }

    // get dag layout of these actors
    let dagNodeMap = new Map();
    for (let actor of fragmentRepresentedActors) {
      actor.rootNode.actorId = actor.actorId;
      treeBfs(actor.rootNode, (node) => {
        node.actorId = actor.actorId;
      });
      dagNodeMap.set(actor.actorId, { id: actor.actorId, nextNodes: [], actor: actor });
    }
    for (let actor of fragmentRepresentedActors) {
      for (let outputActorNode of actor.output) {
        dagNodeMap.get(actor.actorId).nextNodes.push(
          dagNodeMap.get(outputActorNode.actorId)
        )
      }
    }
    let actorDagNodes = [];
    for (let id of dagNodeMap.keys()) {
      actorDagNodes.push(dagNodeMap.get(id));
    }

    let actorsList = getConnectedComponent(actorDagNodes);


    let y = baseY;
    for (let actorDagList of actorsList) {
      let flowChart = this.drawFlow({
        g: g,
        baseX: baseX,
        baseY: y,
        actorDagList: actorDagList
      })
      y += (flowChart.height + gapBetweenFlowChart);
    }
  }
}

/**
 * create a graph view based on raw input from the meta node, 
 * and append the svg component to the giving svg group.
 * @param {d3.Selection} g The parent svg group contain the graph. 
 * @param {any} actorProto Raw response from the meta node. e.g. {node: {...}, actors: {...}}
 * @param {(clickEvent, node) => void} onNodeClick callback when a node (operator) is clicked.
 * @returns void
 */
export default function createView(g, actorProto, onNodeClick){
  return new StreamChartHelper(g, actorProto, onNodeClick).drawManyFlow();
}