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
const two = [
  ["#6FE0D3", "#E09370"],
  ["#75D0E0", "#E0A575"],
  ["#75BAE0", "#E0B175"],
  ["#77A5E0", "#E0BC77"],
  ["#768CE0", "#E0C577"],
  ["#7575E0", "#E0CD75"],
  ["#A978E0", "#E0DF79"],
  ["#C977E0", "#B9E077"],
  ["#E072D7", "#92E072"],
  ["#E069A4", "#69E069"],
  ["#E06469", "#65E086"],
  ["#E07860", "#60E0B2"],
  ["#E08159", "#5AE0CE"],
  ["#E09C5C", "#5CC5E0"],
  ["#E0B763", "#6395E0"],
  ["#E0CE5A", "#6B5AE0"],
  ["#C8E051", "#AA51E0"],
  ["#92E06F", "#E070DB"],
  ["#79E085", "#E07998"],
  ["#80E0B1", "#E08C80"],
  ["#91DBE0", "#E0B292"],
]

const twoGradient = [
  ["#1976d2", "#a6c9ff"],
  ["#FFF38A", "#E0D463"],
  ["#A3ACFF", "#7983DF"],
  ["#A6C9FF", "#7BA3DF"],
  ["#FFBE8C", "#E09B65"],
  ["#FFD885", "#E0B65D"],
  ["#9EE2FF", "#73BEDF"],
  ["#DAFF8F", "#B8E066"],
  ["#FFC885", "#E0A65D"],
  ["#9EFCFF", "#74DCDF"],
  ["#FBFF8C", "#DBE065"],
  ["#9CFFDE", "#71DFBB"],
  ["#FFAF91", "#E08869"],
  ["#B699FF", "#9071E0"],
  ["#9EFFB6", "#74DF8F"],
  ["#FFA19C", "#E07872"],
  ["#AEFF9C", "#85DF71"],
  ["#FF96B9", "#E06D94"],
  ["#FFE785", "#E0C75F"],
  ["#FF94FB", "#E06BDC"],
  ["#DA99FF", "#B66FE0"],
  ["#8F93FF", "#666AE0"],
]

const five = [
  ["#A8936C", "#F5D190", "#8B84F5", "#9AA84A", "#E1F578"],
  ["#A87C6A", "#F5AB8E", "#82CBF5", "#A89348", "#F5D876"],
  ["#A87490", "#F59DCB", "#90F5C7", "#A87752", "#F5B584"],
  ["#856FA8", "#B995F5", "#BAF58A", "#A84D5B", "#F57D8E"],
  ["#7783A8", "#A2B4F5", "#F5EE95", "#9C56A8", "#E589F5"],
  ["#74A895", "#9DF5D4", "#F5BF91", "#526CA8", "#84A6F5"],
  ["#74A878", "#9DF5A3", "#F5A290", "#5298A8", "#84DFF5"],
  ["#94A877", "#D2F5A2", "#F596B6", "#56A88C", "#89F5D0"],
  ["#A8A072", "#F5E79A", "#CD8DF5", "#5DA851", "#92F582"],
  ["#A89176", "#F5CD9F", "#92A3F5", "#A8A554", "#F5F087"],
  ["#A8726A", "#F59B8E", "#83ECF5", "#A88948", "#F5CB76"],
]
export function TwoColor(index) {
  return two[index % two.length]
}

export function FiveColor(index) {
  return five[index % five.length]
}

let s = Math.random() * 100
export function TwoGradient(index) {
  return twoGradient[(Math.round(s) + index) % two.length]
}
