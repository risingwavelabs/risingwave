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
import * as d3 from "d3";
import { svg } from "d3";

export class DrawElement {
  /**
   * @param {{svgElement: d3.Selection<any, any, any, any>}} props 
   */
  constructor(props) {
    /**
     * @type {{svgElement: d3.Selection<any, any, any, any>}}
     */
    this.props = props;
    this.appendFunc = {
      "g": this._appendGroup,
      "circle": this._appendCircle,
      "rect": this._appendRect,
      "text": this._appendText,
      "path": this._appendPath,
      "polygon": this._appendPolygan,
    }
  }

  _appendGroup = () => {
    return new Group({ svgElement: this.props.svgElement.append("g") });
  }

  _appendCircle = () => {
    return new Circle({ svgElement: this.props.svgElement.append("circle") });
  }

  _appendRect = () => {
    return new Rectangle({ svgElement: this.props.svgElement.append("rect") });
  }

  _appendText = () => {
    return new Text({ svgElement: this.props.svgElement.append("text") });
  }

  _appendPath = () => {
    return new Path({ svgElement: this.props.svgElement.append("path") });
  }

  _appendPolygan = () => {
    return new Polygan({ svgElement: this.props.svgElement.append("polygon") });
  }

  on = (event, callback) => {
    this.props.svgElement.on(event, callback);
    return this;
  }

  style = (key, value) => {
    this.props.svgElement.style(key, value);
    return this;
  }

  classed = (clazz, flag) => {
    this.props.svgElement.classed(clazz, flag);
    return this;
  }

  attr = (key, value) => {
    this.props.svgElement.attr(key, value);
    return this;
  }

  append = (type) => {
    return this.appendFunc[type]();
  }
}

export class Group extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<SVGGElement, any, null, undefined>}} props 
   */
  constructor(props) {
    super(props)
  }
}

export class Rectangle extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<SVGRectElement, any, null, any>}} props 
   */
  constructor(props) {
    super(props)
  }
}

export class Circle extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<SVGCircleElement, any, any, any>}} props 
   */
  constructor(props) {
    super(props)
  }
}

export class Text extends DrawElement {
  /**
   * @param {{svgElement: d3.Selection<d3.Selection<any, any, any, any>, any, null, undefined>}} props 
   */
  constructor(props) {
    super(props)
    this.props = props;
  }

  text(content) {
    this.props.svgElement.text(content);
    return this;
  }

  getWidth() {
    return this.props.svgElement.node().getComputedTextLength();
  }
}

export class Polygan extends DrawElement {
  constructor(props) {
    super(props);
    this.props = props;
  }
}

export class Path extends DrawElement {
  constructor(props) {
    super(props);
  }
}

const originalZoom = new d3.ZoomTransform(0.5, 0, 0);

export class SvgEngine {
  /**
   * @param {{g: d3.Selection<SVGGElement, any, null, undefined>}} props 
   */
  constructor(svgRef, height, width) {

    this.height = height;
    this.width = width;
    this.svgRef = svgRef;

    d3.select(svgRef).selectAll("*").remove();
    this.svg = d3
      .select(svgRef)
      .attr("viewBox", [0, 0, width, height]);

    this._g = this.svg.append("g").attr("class", "top");
    this.topGroup = new Group({ svgElement: this._g });

    this.transform;
    this.zoom = d3.zoom().on("zoom", e => {
      this.transform = e.transform;
      this._g.attr("transform", e.transform);
    });

    this.svg.call(this.zoom)
      .call(this.zoom.transform, originalZoom)
    this.svg.on("pointermove", event => {
      this.transform.invert(d3.pointer(event))
    });

  }

  locateTo(selector) {
    let selection = d3.select(selector);
    if (!selection.empty()) {
      this.svg
        .call(this.zoom)
        .call(
          this.zoom.transform,
          new d3.ZoomTransform(0.7, - 0.7 * selection.attr("x"), 0.7 * (-selection.attr("y") + this.height / 2))
        );
    }
  }

  resetCamera() {
    this.svg.call(this.zoom.transform, originalZoom);
  }

  cleanGraph() {
    d3.select(this.svgRef).selectAll("*").remove();
  }

}