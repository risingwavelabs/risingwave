import { treeBfs } from "../algo";

let cnt = 0;
function generateNewNodeId() {
  return "g" + (++cnt);
}

function getNodeId(nodeProto) {
  return nodeProto.operatorId === undefined ? generateNewNodeId() : "o" + nodeProto.operatorId;
}

class Node {
  constructor(id, actorId, nodeProto) {
    this.id = id;
    /**
     * @type {any}
     */
    this.nodeProto = nodeProto;
    /**
     * @type {Array<Node}
     */
    this.nextNodes = [];
    this.actorId = actorId;
  }
}

class StreamNode extends Node {
  constructor(id, actorId, nodeProto) {
    super(id, actorId, nodeProto);
    /**
     * @type {string}
     */
    this.type = this.parseType(nodeProto);
    /**
     * @type {any}
     */
    this.typeInfo = nodeProto[this.type];
  }

  parseType(nodeProto) {
    let typeReg = new RegExp('.+Node');
    for (let [type, _] of Object.entries(nodeProto)) {
      if (typeReg.test(type)) {
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
  constructor(actorId, output, rootNode, fragmentId, actorIdentifier) {
    /**
     * @type {number}
     */
    this.actorId = actorId;
    /**
     * @type {Array<Node>}
     */
    this.output = output;
    /**
     * @type {Node}
     */
    this.rootNode = rootNode;
    /**
     * @type {number}
     */
    this.fragmentId = fragmentId;
    /**
     * @type {string | number}
     */
    this.actorIdentifier = actorIdentifier;
  }
}


export default class StreamPlanParser {
  /**
   * 
   * @param {[{node: any, actors: []}]} data raw response from the meta node
   */
  constructor(data) {

    this.parsedNodeMap = new Map();
    this.parsedActorMap = new Map();
    this.actorId2Proto = new Map();
    /**
     * @type {Set<Actor>}
     * @private
     */
    this.actorIdTomviewNodes = new Map();

    for (let computeNodeData of data) {
      for (let singleActorProto of computeNodeData.actors) {
        this.actorId2Proto.set(singleActorProto.actorId, {
          actorIdentifier: `${computeNodeData.node.host.host}:${computeNodeData.node.host.port}`,
          ...singleActorProto
        });
      }
    }

    for (let [_, singleActorProto] of this.actorId2Proto.entries()) {
      this.parseActor(singleActorProto);
    }

    this.parsedActorList = [];
    for (let [_, actor] of this.parsedActorMap.entries()) {
      this.parsedActorList.push(actor);
    }

    /**
     * @type {Map<number, Array<number>}
     */
    this.mvTableIdToActorList = this._constructMvList();
  }

  _constructMvList() {
    let mvTableIdToActorList = new Map();
    let shellNodes = new Map();
    const getShellNode = (actorId) => {
      if (shellNodes.has(actorId)) {
        return shellNodes.get(actorId);
      }
      let shellNode = {
        id: actorId,
        nextNodes: []
      };
      for (let node of this.parsedActorMap.get(actorId).output) {
        getShellNode(node.actorId).nextNodes.push(shellNode);
      }
      shellNodes.set(actorId, shellNode);
      return shellNode;
    }
    for (let actor of this.parsedActorList) {
      getShellNode(actor.actorId);
    }

    for (let [actorId, mviewNode] of this.actorIdTomviewNodes.entries()) {
      let list = [];
      let shellNode = getShellNode(actorId);
      treeBfs(shellNode, (n) => {
        list.push(n.id)
        if (shellNode.id !== n.id && this.actorIdTomviewNodes.has(n.id)) {
          return true; // stop to traverse its next nodes
        }
      })
      mvTableIdToActorList.set(mviewNode.typeInfo.tableRefId.tableId, list);
    }
    
    return mvTableIdToActorList;
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

    let actor = new Actor(actorId, [], null, actorProto.fragmentId, actorProto.actorIdentifier);

    let rootNode;
    this.parsedActorMap.set(actorId, actor);
    if (actorProto.dispatcher && actorProto.dispatcher.type) {
      let nodeBeforeDispatcher = this.parseNode(actor.actorId, actorProto.nodes);
      rootNode = this.newDispatcher(actor.actorId, actorProto.dispatcher.type, actorProto.downstreamActorId);
      rootNode.nextNodes = [nodeBeforeDispatcher];
    } else {
      rootNode = this.parseNode(actorId, actorProto.nodes);
    }
    actor.rootNode = rootNode;

    return actor;
  }

  parseNode(actorId, nodeProto) {
    let id = getNodeId(nodeProto);
    if (this.parsedNodeMap.has(id)) {
      return this.parsedNodeMap.get(id);
    }
    let newNode = new StreamNode(id, actorId, nodeProto);
    this.parsedNodeMap.set(id, newNode);

    if (nodeProto.input !== undefined) {
      for (let nextNodeProto of nodeProto.input) {
        newNode.nextNodes.push(this.parseNode(actorId, nextNodeProto));
      }
    }

    if (newNode.type === "mergeNode" && newNode.typeInfo.upstreamActorId) {
      for (let upStreamActorId of newNode.typeInfo.upstreamActorId) {
        this.parseActor(this.actorId2Proto.get(upStreamActorId)).output.push(newNode);
      }
    }

    if (newNode.type === "chainNode" && newNode.typeInfo.upstreamActorIds) {
      for (let upStreamActorId of newNode.typeInfo.upstreamActorIds) {
        this.parseActor(this.actorId2Proto.get(upStreamActorId)).output.push(newNode);
      }
    }

    if (newNode.type === "mviewNode") {
      this.actorIdTomviewNodes.set(actorId, newNode);
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
   * @returns {Array<Actor>}
   */
  getParsedActorList() {
    return this.parsedActorList;
  }
}