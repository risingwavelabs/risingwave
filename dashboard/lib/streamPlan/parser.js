class Node {
  constructor(actorId, nodeProto) {
    this.id = nodeProto.operatorId;
    this.nodeProto = nodeProto;
    this.nextNodes = [];
    this.actorId = actorId;
  }
}

class StreamNode extends Node {
  constructor(actorId, nodeProto) {
    super(actorId, nodeProto);
    this.type = this.parseType(nodeProto);
    this.typeInfo = nodeProto[this.type];
  }

  parseType(nodeProto) {
    let types = ["tableSourceNode", "sourceNode", "projectNode", "filterNode",
      "mviewNode", "simpleAggNode", "hashAggNode", "topNNode",
      "hashJoinNode", "mergeNode", "exchangeNode", "chainNode", "localSimpleAggNode"];
    for (let type of types) {
      if (type in nodeProto) {
        return type;
      }
    }
  }
}

class Dispatcher extends Node {
  constructor(actorId, dispatcherType, downstreamActorId, nodeProto) {
    super(actorId, nodeProto);
    this.dispatcherType = dispatcherType;
    this.downstreamActorId = downstreamActorId;
  }
}

class Actor {
  constructor(actorId, output, rootNode, fragmentId) {
    this.actorId = actorId;
    this.output = output;
    this.rootNode = rootNode;
    this.fragmentId = fragmentId;
  }
}


export default class StreamPlanParser {
  /**
   * 
   * @param {{node: any, actors: []}} streamPlan raw response from the meta node
   */
  constructor(streamPlan) {
    this.parsedNodeMap = new Map();
    this.parsedActorMap = new Map();
    this.actorId2Proto = new Map();


    this.node = streamPlan.node;
    this.actorProtoList = streamPlan.actors;
    for (let actorProto of this.actorProtoList) {
      this.actorId2Proto.set(actorProto.actorId, actorProto);
    }
    for (let actorProto of this.actorProtoList) {
      this.parseActor(actorProto);
    }
    this.parsedActorList = [];
    for (let actorId of this.parsedActorMap.keys()) {
      this.parsedActorList.push(this.parsedActorMap.get(actorId));
    }
    console.log(this.parsedActorList);
  }

  newDispatcher(actorId, type, downstreamActorId) {
    return new Dispatcher(actorId, type, downstreamActorId, {
      operatorId: 100000 + actorId
    })
  }

  /**
   * Parse raw data from meta node to an actor
   * @param {{
   *  actorId: number, 
   *  fragmentId: number, 
   *  nodes: any, 
   *  dispatcher?: {type: string}, 
   *  downstreamActorId?: any
   * }} actorProto 
   * @returns {Actor}
   */
  parseActor(actorProto) {
    let actorId = actorProto.actorId;
    if (this.parsedActorMap.has(actorId)) {
      return this.parsedActorMap.get(actorId);
    }

    let actor = new Actor(actorId, [], null, actorProto.fragmentId);
    let rootNode
    this.parsedActorMap.set(actorId, actor);
    if (actorProto.dispatcher && actorProto.dispatcher.type) {
      let nodeBeforeDispatcher = this.parseNode(actor.actorId, actorProto.nodes);
      rootNode = this.newDispatcher(actor.actorId, actorProto.dispatcher.type, actorProto.downstreamActorId);
      rootNode.nextNodes = [nodeBeforeDispatcher];
    } else {
      rootNode = this.parseNode(actorId, actorProto.nodes);
    }

    actor.rootNode = rootNode;
    return this.parsedActorMap.get(actorId);
  }

  parseNode(actorId, nodeProto) {
    let id = nodeProto.operatorId;
    if (this.parsedNodeMap.has(id)) {
      return this.parsedNodeMap.get(id);
    }
    let newNode = new StreamNode(actorId, nodeProto);
    this.parsedNodeMap.set(id, newNode);

    if (nodeProto.input !== undefined) {
      for (let nextNodeProto of nodeProto.input) {
        newNode.nextNodes.push(this.parseNode(actorId, nextNodeProto));
      }
    }

    if (newNode.type === "mergeNode") {
      for (let upStreamActorId of newNode.typeInfo.upstreamActorId) {
        this.parseActor(this.actorId2Proto.get(upStreamActorId)).output.push(newNode);
      }
    }

    if (newNode.type === "chainNode") {
      for (let upStreamActorId of newNode.typeInfo.upstreamActorIds) {
        this.parseActor(this.actorId2Proto.get(upStreamActorId)).output.push(newNode);
      }
    }

    return newNode;
  }

  getActor(actorId) {
    return this.parsedActorMap.get(actorId);
  }

  getOperator(operatorId) {
    return this.parsedNodeMap.get(operatorId);
  }

  /**
   * @returns information about computation node
   */
  getNodeInformation() {
    return this.node;
  }

  /**
   * @returns {Array<Actor>}
   */
  getParsedActorList() {
    return this.parsedActorList;
  }
}