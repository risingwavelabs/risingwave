/*
 * Copyright 2022 Singularity Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
import { Node } from "../lib/algo"
import { StreamChartHelper } from "../lib/streamPlan/streamChartHelper"

describe("Algo", () => {
  it("should generate right dag layout", () => {
    // fake data
    let nodes = []
    for (let i = 0; i < 10; ++i) {
      nodes.push(new Node([], i + 1))
    }
    const n = (i) => nodes[i - 1]
    n(1).nextNodes = [n(2), n(3)]
    n(2).nextNodes = [n(9)]
    n(3).nextNodes = [n(5), n(10)]
    n(4).nextNodes = [n(5)]
    n(5).nextNodes = [n(6), n(7)]
    n(6).nextNodes = [n(9), n(10)]
    n(7).nextNodes = [n(8)]

    let dagPositionMapper = new StreamChartHelper().dagLayout(nodes)

    // construct map
    let maxLayer = 0
    let maxRow = 0
    for (let node of dagPositionMapper.keys()) {
      let pos = dagPositionMapper.get(node)
      maxLayer = pos[0] > maxLayer ? pos[0] : maxLayer
      maxRow = pos[1] > maxRow ? pos[1] : maxRow
    }
    let m = []
    for (let i = 0; i < maxLayer + 1; ++i) {
      m.push([])
      for (let r = 0; r < maxRow + 1; ++r) {
        m[i].push([])
      }
    }
    for (let node of dagPositionMapper.keys()) {
      let pos = dagPositionMapper.get(node)
      m[pos[0]][pos[1]] = node
    }

    // search
    const _search = (l, r, d) => {
      // Layer, Row
      if (l > maxLayer || r > maxRow || r < 0) {
        return false
      }
      if (m[l][r].id !== undefined) {
        return m[l][r].id === d
      }
      return _search(l + 1, r, d)
    }

    const canReach = (node, nextNode) => {
      let pos = dagPositionMapper.get(node)
      for (let r = 0; r <= maxRow; ++r) {
        if (_search(pos[0] + 1, r, nextNode.id)) {
          return true
        }
      }
      return false
    }

    //check all links
    let ok = true
    for (let node of nodes) {
      for (let nextNode of node.nextNodes) {
        if (!canReach(node, nextNode)) {
          console.error(
            `Failed to connect node ${node.id} to node ${nextNode.id}`
          )
          ok = false
          break
        }
      }
      if (!ok) {
        break
      }
    }

    // visualization
    // let s = "";
    // for(let r = maxRow; r >= 0; --r){
    //   for(let l = 0; l <= maxLayer; ++l){
    //     s += `\t${m[l][r].id ? m[l][r].id : " "}`
    //   }
    //   s += "\n"
    // }
    // console.log(s);

    expect(ok).toEqual(true)
  })
})
