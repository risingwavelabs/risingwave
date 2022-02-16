import { newMatrix } from "./util";

/**
 * Traverse a tree from its root node, and do operation
 * by calling the step function.
 * Every node will be visted only once.
 * @param {{nextNodes: []}} root The root node of the tree
 * @param {(node: any) => boolean} step callback when a node is visited.
 * return true if you want to stop to traverse its next nodes.
 */
export function treeBfs(root, step) {
  let bfsList = [root];
  while (bfsList.length !== 0) {
    let c = bfsList.shift();

    if (!step(c)) {
      for (let nextNode of c.nextNodes) {
        bfsList.push(nextNode);
      }
    }
  }
}


/**
 * Traverse a graph from a random node, and do
 * operation by calling the step function.
 * Every node will be visted only once.
 * @param {{nextNodes: []}} root A random node in the graph
 * @param {(node: any) => boolean} step callback when a node is visited.
 * @param {string} [neighborListKey="nextNodes"] 
 * return true if you want to stop traverse its next nodes
 */
export function graphBfs(root, step, neighborListKey) {
  let key = neighborListKey || "nextNodes";
  let visitedNodes = new Set();
  let bfsList = [root];
  while (bfsList.length !== 0) {
    let c = bfsList.shift();

    visitedNodes.add(c);
    if (!step(c)) {
      for (let nextNode of c[key]) {
        if (!visitedNodes.has(nextNode)) {
          bfsList.push(nextNode);
        }
      }
    }
  }
}


/**
 * Group nodes in the same connected component. The method will not
 * change the input. The ouput contains the original references.
 * @param {Array<{nextNodes: []}>} nodes 
 * @returns {Array<Array<any>>} A list of groups containing 
 * nodes in the same connected component
 */
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