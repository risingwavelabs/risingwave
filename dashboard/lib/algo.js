import { iter, newMatrix } from "./util";

export class Node {
  /**
   * @param {Array<Node>} nextNodes 
   * @param {number} id
   */
  constructor(nextNodes, id) {
    this.nextNodes = nextNodes;
    this.id = id;
  }
}

/**
 * Traverse a tree from its root node, and do operation
 * by calling the setp function.
 * @param {Node} root 
 * @param {(node: Node) => void} step callback when a node is visited.
 */
export function treeBfs(root, step) {
  let bfsList = [root];
  while (bfsList.length !== 0) {
    let c = bfsList.shift();

    step(c);

    for (let nextNode of c.nextNodes) {
      bfsList.push(nextNode);
    }
  }
}

export function graphBfs(root, step) {
  let visitedNodes = new Set();
  let bfsList = [root];
  while (bfsList.length !== 0) {
    let c = bfsList.shift();

    step(c);
    visitedNodes.add(c);

    for (let nextNode of c.nextNodes) {
      if (!visitedNodes.has(nextNode)) {
        bfsList.push(nextNode);
      }
    }
  }
}

/**
 * 0, -1, 1, -2, 2, -3, 3, -4, 4, -5, 5 ,....
 */
export class OscSeq {
  constructor() {
    this._n = 0;
  }
  next() {
    this._n = this._n > 0 ? -(this._n + 1) : -this._n;
  }
  current() {
    return this.n;
  }
}

export function getConnectedComponent(nodes) {

  let node2shellNodes = new Map();

  for (let node of nodes) {
    let shellNode = {
      val: node,
      nextNodes: [],
      g: -1
    }
    node2shellNodes.set(node, shellNode);
  }

  // make a shell non-directed graph from the original DAG.
  for (let node of nodes) {
    let shellNode = node2shellNodes.get(node);
    for (let nextNode of node.nextNodes) {
      let nextShellNode = node2shellNodes.get(nextNode);
      shellNode.nextNodes.push(nextShellNode);
      nextShellNode.nextNodes.push(shellNode);
    }
  }

  // bfs assign group number
  let cnt = 0;
  for (let node of node2shellNodes.keys()) {
    let shellNode = node2shellNodes.get(node);
    if (shellNode.g === -1) {
      shellNode.g = cnt++;
      graphBfs(shellNode, (c) => {
        c.g = shellNode.g
      });
    }
  }

  let group = newMatrix(cnt);
  for (let node of node2shellNodes.keys()) {
    let shellNode = node2shellNodes.get(node);
    group[shellNode.g].push(node);
  }

  return group;
}