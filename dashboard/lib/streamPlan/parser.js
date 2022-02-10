// TODO: move these three to StreamPlanParser properties
var parsedNodeMap = new Map();
var parsedActorMap = new Map();
var actorId2Proto = new Map();

class Node {
  constructor(actorId, nodeProto) {
    this.id = nodeProto.operatorId;
    this.nodeProto = nodeProto;
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
    let types = ["tableSourceNode", "sourceNode", "projectNode", "filterNode",
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

class Dispatcher extends Node {
  constructor(actorId, dispatcherType, downstreamActorId, nodeProto) {
    super(actorId, nodeProto);
    this.dispatcherType = dispatcherType;
    this.downstreamActorId = downstreamActorId;
  }

  static newDispatcher(actorId, type, downstreamActorId) {
    return new Dispatcher(actorId, type, downstreamActorId, {
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
    let rootNode = Dispatcher.newDispatcher(actor.actorId, actorProto.dispatcher.type, actorProto.downstreamActorId);
    rootNode.nextNodes = [nodeBeforeDispatcher];
    actor.rootNode = rootNode;
    return parsedActorMap.get(actorId);
  }
}


export default class StreamPlanParser {
  /**
   * 
   * @param {*} streamPlan raw input from the meta node
   */
  constructor(streamPlan) {
    parsedNodeMap = new Map();
    parsedActorMap = new Map();
    actorId2Proto = new Map();

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

  getActor(actorId) {
    return parsedActorMap.get(actorId);
  }

  getOperator(operatorId) {
    return parsedNodeMap.get(operatorId);
  }

  /**
   * @returns information about computation node
   */
  getNodeInformation() {
    return this.node;
  }

  /**
   * 
   * @returns {Array<Actor>}
   */
  getParsedActorList() {
    return this.parsedActorList;
  }
}