import * as d3 from "d3";
import * as color from "./color";
import { getConnectedComponent, treeBfs } from "./algo";
import { cloneDeep, max } from "lodash";
import { newNumberArray } from "./util";
// d3.line().curve(d3.curveLinear)([pointe, ...])
// Actor constant
const nodeRadius = 30; // the radius of the tree nodes in an actor
const nodeStrokeWidth = 5; // the stroke width of the link of the tree nodes in an actor
const widthUnit = 150; // the width of a tree node in an actor
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

// Draw link effect
const DrawLinkEffect = true;
/*
                                o--bendGap--o-----------o
                                |
                                |
                                |
  o--connectionGap--o--bendGap--o

  [ o: point]
  [ -: line ]

  try example at: http://bl.ocks.org/d3indepth/b6d4845973089bc1012dec1674d3aff8
*/
const bendGap = 50;
const connectionGap = 50;

function constructNodeLinkId(node1, node2) {
  return "node-" + (node1.id > node2.id ? node1.id + "-" + node2.id : node2.id + "-" + node1.id);
}

function constructNodeId(node) {
  return "node-" + node.id;
}

const parsedNodeMap = new Map();
const parsedActorMap = new Map();
const actorId2Proto = new Map();

class Node {
  constructor(actorId, nodeProto) {
    this.id = nodeProto.operatorId;
    this.nextNodes = [];
    this.actorId = actorId;
    if (nodeProto.input !== undefined) {
      for (let nextNodeProto of nodeProto.input) {
        let nextNode = StreamNode.parseNode(actorId, nextNodeProto);
        this.nextNodes.push(nextNode);
      }
    }
  }
}

class StreamNode extends Node {
  constructor(actorId, nodeProto) {
    super(actorId, nodeProto);
    this.type = this.paseType(nodeProto);
    this.typeInfo = nodeProto[this.type];

    if (this.type === "mergeNode") {
      for (let upStreamActorId of this.typeInfo.upstreamActorId) {
        Actor.parseActor(actorId2Proto.get(upStreamActorId)).output.push(this);
      }
    }
  }

  paseType(nodeProto) {
    let types = ["tableSourceNode", "projectNode", "filterNode",
      "mviewNode", "simpleAggNode", "hashAggNode", "topNNode",
      "hashJoinNode", "mergeNode", "exchangeNode", "chainNode"];
    for (let type of types) {
      if (type in nodeProto) {
        return type;
      }
    }
  }

  static parseNode(actorId, nodeProto) {
    let id = nodeProto.operatorId;
    if (parsedNodeMap.has(id)) {
      return parsedNodeMap.get(id);
    }
    parsedNodeMap.set(id, new StreamNode(actorId, nodeProto));
    return parsedNodeMap.get(id);
  }
}

class Dispatcher extends Node{
  constructor(actorId, dispatcherType, nodeProto){
    super(actorId, nodeProto);
    this.dispatcherType = dispatcherType;
  }

  static newDispatcher(actorId, type){
    return new Dispatcher(actorId, type, {
      operatorId: 100000 + actorId
    })
  }
}

class Actor {
  constructor(actorId, output, rootNode, fragmentIdentifier) {
    this.actorId = actorId;
    this.output = output;
    this.rootNode = rootNode;
    this.fragmentIdentifier = fragmentIdentifier;
  }

  static parseActor(actorProto) {
    let actorId = actorProto.actorId;
    if (parsedActorMap.has(actorId)) {
      return parsedActorMap.get(actorId);
    }

    let actor = new Actor(actorId, [], null, actorProto.nodes.operatorId);
    parsedActorMap.set(actorId, actor);
    let nodeBeforeDispatcher = StreamNode.parseNode(actor.actorId, actorProto.nodes);
    let rootNode = Dispatcher.newDispatcher(actor.actorId, actorProto.dispatcher.type);
    rootNode.nextNodes = [nodeBeforeDispatcher];
    actor.rootNode = rootNode;
    return parsedActorMap.get(actorId);
  }
}

class StreamPlanPaser {
  constructor(streamPlan) {
    this.node = streamPlan.node;
    this.actorProtoList = streamPlan.actors;
    for (let actorProto of this.actorProtoList) {
      actorId2Proto.set(actorProto.actorId, actorProto);
    }
    for (let actorProto of this.actorProtoList) {
      Actor.parseActor(actorProto);
    }
    this.parsedActorList = [];
    for (let actorId of parsedActorMap.keys()) {
      this.parsedActorList.push(parsedActorMap.get(actorId));
    }
  }
  getNodeInformation() {
    return this.node;
  }
  getParsedActorList() {
    return this.parsedActorList;
  }
}

/**
 * Topological sort
 * @param {Array<Node>} nodes An array of node: {nextNodes: [...]} 
 * @returns {Map<Node, [number, number]>} position of each node
 */
function dagLayout(nodes) {
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
        node2dagNode.get(nextNode).top = false;
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
    let dagNode = { node: node, temp: false, perm: false, top: true }; // top: flag for input nodes (no incoming edges)
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
    node.g = node.top ? 0 : (maxLayer - node.g);
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

function calculateActorBoxSize(rootNode) {
  let rootNodeCopy = cloneDeep(rootNode);
  return layoutActorBox(rootNodeCopy, 0, 0);
}

/**
 * Calculate the position of each node in the actor box.
 * This will change the node's position
 * @param {Node} rootNode 
 * @param {number} baseX 
 * @param {number} baseY  
 * @returns The size [width, height] of the actor box
 */
function layoutActorBox(rootNode, baseX, baseY) {
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
 * 
 * @param {d3.Selection} props.g The target group contains this tree.
 * @param {object} props.rootNode The root node of the tree in the actor
 * @param {String} props.nodeColor [optinal] The filled color of nodes.
 * @param {String} props.strokeColor [optinal] The color of the stroke.
 * @param {Function} props.onNodeClicked [optinal] The callback function when a node is clicked.
 * @param {Function} props.onMouseOver [optinal] The callback function when the mouse enters a node.
 * @param {Function} props.onMouseOut [optinal] The callback function when the mouse leaves a node.
 * @param {number} props.baseX [optinal] The x coordinate of the lef-top corner
 * @param {number} props.baseY [optinal] The y coordinate of the lef-top corner
 * @returns {d3.Selection} The group element of this tree
 */
export function drawActorBox(props) {

  if (props.g === undefined) {
    throw Error("Invalid Argument: Target group cannot be undefined.");
  }

  const actorId = props.actorId;
  const group = props.g.append("g");
  const rootNode = props.rootNode || [];
  const onNodeClicked = props.onNodeClicked;
  const onMouseOut = props.onMouseOut;
  const onMouseOver = props.onMouseOver;
  const baseX = props.x === undefined ? 0 : props.x;
  const baseY = props.y === undefined ? 0 : props.y;
  const backgroundColor = props.backgroundColor || "none";
  const nodeColor = props.nodeColor || "black";
  const strokeColor = props.strokeColor || "white";
  const boxStrokeColor = props.boxStrokeColor || "gray";
  const linkColor = props.linkColor || "gray";

  const [boxWidth, boxHeight] = calculateActorBoxSize(rootNode);
  layoutActorBox(rootNode, baseX + boxWidth - actorBoxPadding, baseY + boxHeight / 2);

  // draw box
  group.attr("id", "actor-" + actorId);
  group.append("rect")
    .attr("width", boxWidth)
    .attr("height", boxHeight)
    .attr("x", baseX)
    .attr("y", baseY)
    .attr("fill", backgroundColor)
    .attr("rx", 20)
    .attr("stroke-width", actorBoxStroke)
    .attr("stroke", boxStrokeColor)
  group.append("text")
    .attr("x", baseX)
    .attr("y", baseY)
    .attr("font-size", 50)
    .text(actorId);

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
      .attr('fill', nodeColor)
      .style('pointer', 'cursor')
      .style('stroke-width', nodeStrokeWidth)
      .on("click", () => onNodeClicked && onNodeClicked(node))
      .on("mouseover", () => onMouseOver && onMouseOver(node))
      .on("mouseout", () => onMouseOut && onMouseOut(node))
  })

  // dataflow effect
  if (DrawLinkEffect) {
    group.selectAll("path")
      .attr("stroke-dasharray", linkStrokeDash);
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
 * @param {d3.Selection} g The target group contains this group.
 * @param {Arrary} actors The actors in the same flow.
 * actors = [
 *   {
 *     actorId: 1,
 *     rootNode: Node,
 *     output: [Node, Node]
 *   },
 * ]
 * @returns {d3.Selection} 
 */
export function drawFlow(props) {

  if (props.g === undefined) {
    throw Error("Invalid Argument: Target group cannot be undefined.");
  }

  const g = props.g;
  const actorDagList = props.actorDagList || [];
  const baseX = props.baseX || 0;
  const baseY = props.baseY || 0;

  console.log(baseX, baseY);
  
  let layoutPositionMapper = dagLayout(actorDagList);
  const actors = [];
  for(let actorDag of actorDagList){
    actors.push(actorDag.actor);
  }

  // calculate actor box size
  for (let actor of actors) {
    [actor.boxWidth, actor.boxHeight] = calculateActorBoxSize(actor.rootNode);
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


  let cnt = 0;
  let actorColorMap = new Map();
  for (let actor of actors) {
    let c = color.TwoGradient(cnt);
    actorColorMap.set(actor.actorId, [c[0], c[1]]);
    cnt++;
  }
  let actorBoxList = [];
  for (let actor of actors) {
    let actorBox = drawActorBox({
      actorId: actor.actorId,
      g: fragmentLayer,
      rootNode: actor.rootNode,
      x: baseX + layer2x[actor.layer],
      y: baseY + row2y[actor.row],
      nodeColor: actorColorMap.get(actor.actorId)[0],
      strokeColor: "white",
      backgroundColor: actorColorMap.get(actor.actorId)[1],
      linkColor: "white",
      boxStrokeColor: actorColorMap.get(actor.actorId)[0]
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
          id: actor.actorId,
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
    .attr("stroke", s => actorColorMap.get(s.id)[0]);
  // dataflow effect
  if (DrawLinkEffect) {
    linkLayer.selectAll("path")
      .attr("stroke-dasharray", "20, 20");
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
  for(let actorBox of actorBoxList){
    
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

export function drawManyFlow(props){
  const g = props.g;
  const actorProto = props.actorProto || [];
  const baseX = props.baseX || 0;
  const baseY = props.baseY || 0;

  console.log(actorProto);

  // remove actors in the same fragment TODO: show these actors in the graph
  const streamPlan = new StreamPlanPaser(actorProto);
  const allActors = streamPlan.getParsedActorList();
  const dispatcherNode2ActorId = new Set();
  const fragmentRepresentedActors = [];
  for(let actor of allActors){
    if( ! dispatcherNode2ActorId.has(actor.fragmentIdentifier)){
      fragmentRepresentedActors.push(actor);
      dispatcherNode2ActorId.add(actor.fragmentIdentifier);
    }
  }

  console.log(fragmentRepresentedActors);

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


  let y = 0;
  for(let actorDagList of actorsList){
    let flowChart = drawFlow({
      g: g,
      baseX: baseX,
      baseY: y,
      actorDagList: actorDagList
    })
    y += (flowChart.height + gapBetweenFlowChart);
  }
}