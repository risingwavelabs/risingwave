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
import { fabric } from "fabric"

// Disable cache to improve performance.
fabric.Object.prototype.objectCaching = false
fabric.Object.prototype.statefullCache = false
fabric.Object.prototype.noScaleCache = true
fabric.Object.prototype.needsItsOwnCache = () => false

export class DrawElement {
  /**
   * @param {{svgElement: d3.Selection<any, any, any, any>}} props
   */
  constructor(props) {
    /**
     * @type {{svgElement: d3.Selection<any, any, any, any>}}
     */
    this.props = props
    if (props.canvasElement) {
      props.engine.canvas.add(props.canvasElement)
      props.canvasElement.on("mouse:down", (e) => {
        console.log(e)
      })
    }

    this.eventHandler = new Map()
  }

  // TODO: this method is for migrating from d3.js to fabric.js.
  // This should be replaced by a more suitable way.
  _attrMap(key, value) {
    return [key, value]
  }

  // TODO: this method is for migrating from d3.js to fabric.js.
  // This should be replaced by a more suitable way.
  attr(key, value) {
    let setting = this._attrMap(key, value)
    if (setting && setting.length === 2) {
      this.props.canvasElement &&
        this.props.canvasElement.set(setting[0], setting[1])
    }
    return this
  }

  _afterPosition() {
    let ele = this.props.canvasElement
    ele && this.props.engine._addDrawElement(this)
  }

  // TODO: this method is for migrating from d3.js to fabric.js.
  // This should be replaced by a more suitable way.
  position(x, y) {
    this.props.canvasElement.set("left", x)
    this.props.canvasElement.set("top", y)
    this._afterPosition()
    return this
  }

  on(event, callback) {
    this.eventHandler.set(event, callback)
    return this
  }

  getEventHandler(event) {
    return this.eventHandler.get(event)
  }

  // TODO: this method is for migrating from d3.js to fabric.js.
  // This should be replaced by a more suitable way.
  style(key, value) {
    return this.attr(key, value)
  }

  classed(clazz, flag) {
    this.props.engine.classedElement(clazz, this, flag)
    return this
  }
}

export class Group extends DrawElement {
  /**
   * @param {{engine: CanvasEngine}} props
   */
  constructor(props) {
    super(props)

    this.appendFunc = {
      g: this._appendGroup,
      circle: this._appendCircle,
      rect: this._appendRect,
      text: this._appendText,
      path: this._appendPath,
      polygon: this._appendPolygan,
    }

    this.basicSetting = {
      engine: props.engine,
    }
  }

  _appendGroup = () => {
    return new Group(this.basicSetting)
  }

  _appendCircle = () => {
    return new Circle({
      ...this.basicSetting,
      ...{
        canvasElement: new fabric.Circle({
          selectable: false,
          hoverCursor: "pointer",
        }),
      },
    })
  }

  _appendRect = () => {
    return new Rectangle({
      ...this.basicSetting,
      ...{
        canvasElement: new fabric.Rect({
          selectable: false,
          hoverCursor: "pointer",
        }),
      },
    })
  }

  _appendText = () => {
    return (content) =>
      new Text({
        ...this.basicSetting,
        ...{
          canvasElement: new fabric.Text(content || "undefined", {
            selectable: false,
            textAlign: "justify-center",
          }),
        },
      })
  }

  _appendPath = () => {
    return (d) =>
      new Path({
        ...this.basicSetting,
        ...{
          canvasElement: new fabric.Path(d, { selectable: false }),
        },
      })
  }

  _appendPolygan = () => {
    return new Polygan(this.basicSetting)
  }

  append = (type) => {
    return this.appendFunc[type]()
  }
}

export class Rectangle extends DrawElement {
  /**
   * @param {{g: fabric.Group}} props
   */
  constructor(props) {
    super(props)
    this.props = props
  }

  init(x, y, width, height) {
    let ele = this.props.canvasElement
    ele.set("left", x)
    ele.set("top", y)
    ele.set("width", width)
    ele.set("height", height)
    super._afterPosition()
    return this
  }

  _attrMap(key, value) {
    if (key === "rx") {
      this.props.canvasElement.set("rx", value)
      this.props.canvasElement.set("ry", value)
      return false
    }
    return [key, value]
  }
}

export class Circle extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<SVGCircleElement, any, any, any>}} props
   */
  constructor(props) {
    super(props)
    this.props = props
    this.radius = 0
  }

  init(x, y, r) {
    this.props.canvasElement.set("left", x - r)
    this.props.canvasElement.set("top", y - r)
    this.props.canvasElement.set("radius", r)
    super._afterPosition()
    return this
  }

  _attrMap(key, value) {
    if (key === "r") {
      this.radius = value
      return ["radius", value]
    }
    if (key === "cx") {
      return ["left", value - this.radius]
    }
    if (key === "cy") {
      return ["top", value - this.radius]
    }
    return [key, value]
  }
}

export class Text extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<d3.Selection<any, any, any, any>, any, null, undefined>}} props
   */
  constructor(props) {
    super(props)
    this.props = props
  }

  position(x, y) {
    let e = this.props.canvasElement
    e.set("top", y)
    e.set("left", x)
    super._afterPosition()
    return this
  }

  _attrMap(key, value) {
    if (key === "text-anchor") {
      return ["textAlign", value]
    }
    if (key === "font-size") {
      return ["fontSize", value]
    }
    return [key, value]
  }

  text(content) {
    return this
  }

  getWidth() {}
}

export class Polygan extends DrawElement {
  constructor(props) {
    super(props)
    this.props = props
  }
}

export class Path extends DrawElement {
  constructor(props) {
    super(props)
    this.props = props
    this.strokeWidth = 1
    super._afterPosition()
  }

  _attrMap(key, value) {
    if (key === "fill") {
      return ["fill", value === "none" ? false : value]
    }
    if (key === "stroke-width") {
      this.props.canvasElement.set(
        "top",
        this.props.canvasElement.get("top") - value / 2
      )
      return ["strokeWidth", value]
    }
    if (key === "stroke-dasharray") {
      return ["strokeDashArray", value.split(",")]
    }
    if (key === "layer") {
      if (value === "back") {
        this.props.canvasElement.canvas.sendToBack(this.props.canvasElement)
      }
      return false
    }
    return [key, value]
  }
}

// TODO: Use rbtree
class CordMapper {
  constructor() {
    this.map = new Map()
  }

  rangeQuery(start, end) {
    let rtn = new Set()
    for (let [k, s] of this.map.entries()) {
      if (start <= k && k <= end) {
        s.forEach((v) => rtn.add(v))
      }
    }
    return rtn
  }

  insert(k, v) {
    if (this.map.has(k)) {
      this.map.get(k).add(v)
    } else {
      this.map.set(k, new Set([v]))
    }
  }
}

class GridMapper {
  constructor() {
    this.xMap = new CordMapper()
    this.yMap = new CordMapper()
    this.gs = 100 // grid size
  }

  _getKey(value) {
    return Math.round(value / this.gs)
  }

  addObject(minX, maxX, minY, maxY, ele) {
    for (let i = minX; i <= maxX + this.gs; i += this.gs) {
      this.xMap.insert(this._getKey(i), ele)
    }
    for (let i = minY; i <= maxY + this.gs; i += this.gs) {
      this.yMap.insert(this._getKey(i), ele)
    }
  }

  areaQuery(minX, maxX, minY, maxY) {
    let xs = this.xMap.rangeQuery(this._getKey(minX), this._getKey(maxX))
    let ys = this.yMap.rangeQuery(this._getKey(minY), this._getKey(maxY))
    let rtn = new Set()
    xs.forEach((e) => {
      if (ys.has(e)) {
        rtn.add(e)
      }
    })
    return rtn
  }
}

export class CanvasEngine {
  /**
   * @param {string} canvasId The DOM id of the canvas
   * @param {number} height the height of the canvas
   * @param {number} width the width of the canvas
   */
  constructor(canvasId, height, width) {
    let canvas = new fabric.Canvas(canvasId)
    canvas.selection = false // improve performance

    this.height = height
    this.width = width
    this.canvas = canvas
    this.clazzMap = new Map()
    this.topGroup = new Group({ engine: this })
    this.gridMapper = new GridMapper()
    this.canvasElementToDrawElement = new Map()

    let that = this
    canvas.on("mouse:wheel", function (opt) {
      var evt = opt.e
      if (evt.ctrlKey === true) {
        var delta = opt.e.deltaY
        var zoom = canvas.getZoom()
        zoom *= 0.999 ** delta
        if (zoom > 10) zoom = 10
        if (zoom < 0.03) zoom = 0.03
        canvas.zoomToPoint({ x: opt.e.offsetX, y: opt.e.offsetY }, zoom)
        that._refreshView()
        evt.preventDefault()
        evt.stopPropagation()
      } else {
        that.moveCamera(-evt.deltaX, -evt.deltaY)
        evt.preventDefault()
        evt.stopPropagation()
      }
    })

    canvas.on("mouse:down", function (opt) {
      var evt = opt.e
      this.isDragging = true
      this.selection = false
      this.lastPosX = evt.clientX
      this.lastPosY = evt.clientY

      that._handleClickEvent(opt.target)
    })

    canvas.on("mouse:move", function (opt) {
      if (this.isDragging) {
        var e = opt.e
        that.moveCamera(e.clientX - this.lastPosX, e.clientY - this.lastPosY)
        this.lastPosX = e.clientX
        this.lastPosY = e.clientY
      }
    })
    canvas.on("mouse:up", function (opt) {
      this.setViewportTransform(this.viewportTransform)
      this.isDragging = false
      this.selection = true
    })
  }

  /**
   * Move the current view point.
   * @param {number} deltaX
   * @param {number} deltaY
   */
  async moveCamera(deltaX, deltaY) {
    this.canvas.setZoom(this.canvas.getZoom()) // essential for rendering (seems like a bug)
    let vpt = this.canvas.viewportTransform
    vpt[4] += deltaX
    vpt[5] += deltaY
    this._refreshView()
  }

  /**
   * Invoke the click handler of an object.
   * @param {fabric.Object} target
   */
  async _handleClickEvent(target) {
    if (target === null) {
      return
    }
    let ele = this.canvasElementToDrawElement.get(target)
    let func = ele.getEventHandler("click")
    if (func) {
      func()
    }
  }

  /**
   * Set the objects in the current view point visible.
   * And set other objects not visible.
   */
  async _refreshView() {
    const padding = 50 // Make the rendering area a little larger.
    let vpt = this.canvas.viewportTransform
    let zoom = this.canvas.getZoom()
    let cameraWidth = this.width
    let cameraHeight = this.height
    let minX = -vpt[4] - padding
    let maxX = -vpt[4] + cameraWidth + padding
    let minY = -vpt[5] - padding
    let maxY = -vpt[5] + cameraHeight + padding
    let visibleSet = this.gridMapper.areaQuery(
      minX / zoom,
      maxX / zoom,
      minY / zoom,
      maxY / zoom
    )

    this.canvas.getObjects().forEach((e) => {
      if (visibleSet.has(e)) {
        e.visible = true
      } else {
        e.visible = false
      }
    })

    this.canvas.requestRenderAll()
  }

  /**
   * Register an element to the engine. This should
   * be called when a DrawElement instance is added
   * to the canvas.
   * @param {DrawElement} ele
   */
  _addDrawElement(ele) {
    let canvasElement = ele.props.canvasElement
    this.canvasElementToDrawElement.set(canvasElement, ele)
    this.gridMapper.addObject(
      canvasElement.left,
      canvasElement.left + canvasElement.width,
      canvasElement.top,
      canvasElement.top + canvasElement.height,
      canvasElement
    )
  }

  /**
   * Assign a class to an object or remove a class from it.
   * @param {string} clazz class name
   * @param {DrawElement} element target object
   * @param {boolean} flag true if the object is assigned, otherwise
   * remove the class from the object
   */
  classedElement(clazz, element, flag) {
    if (!flag) {
      this.clazzMap.has(clazz) && this.clazzMap.get(clazz).delete(element)
    } else {
      if (this.clazzMap.has(clazz)) {
        this.clazzMap.get(clazz).add(element)
      } else {
        this.clazzMap.set(clazz, new Set([element]))
      }
    }
  }

  /**
   * Move current view point to the object specified by
   * the selector. The selector is the class of the
   * target object for now.
   * @param {string} selector The class of the target object
   */
  locateTo(selector) {
    //
    let selectorSet = this.clazzMap.get(selector)
    if (selectorSet) {
      let arr = Array.from(selectorSet)
      if (arr.length > 0) {
        let ele = arr[0]
        let x = ele.props.canvasElement.get("left")
        let y = ele.props.canvasElement.get("top")
        let scale = 0.6
        this.canvas.setZoom(scale)
        let vpt = this.canvas.viewportTransform
        vpt[4] = (-x + this.width * 0.5) * scale
        vpt[5] = (-y + this.height * 0.5) * scale
        this.canvas.requestRenderAll()
        this._refreshView()
      }
    }
  }

  /**
   * Move current view point to (0, 0)
   */
  resetCamera() {
    let zoom = this.canvas.getZoom()
    zoom *= 0.999
    this.canvas.setZoom(zoom)
    let vpt = this.canvas.viewportTransform
    vpt[4] = 0
    vpt[5] = 0
    this.canvas.requestRenderAll()
    this._refreshView()
  }

  /**
   * Dispose the current canvas. Remove all the objects to
   * free memory. All objects in the canvas will be removed.
   */
  cleanGraph() {
    console.log("clean called")
    this.canvas.dispose()
  }

  /**
   * Resize the canvas. This is called when the browser size
   * is changed, such that the canvas can fix the current
   * size of the browser.
   *
   * Note that the outer div box of the canvas will be set
   * according to the parameters. However, the width and
   * height of the canvas is double times of the parameters.
   * This is the feature of fabric.js to keep the canvas
   * in high resolution all the time.
   *
   * @param {number} width the width of the canvas
   * @param {number} height the height of the canvas
   */
  resize(width, height) {
    this.width = width
    this.height = height
    this.canvas.setDimensions({ width: this.width, height: this.height })
  }
}
