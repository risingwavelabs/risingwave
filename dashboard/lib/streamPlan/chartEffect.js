// only use the DOM information to control DOM.

import * as d3 from "d3";

const highLightColor = "#DC143C";
const linkFlowEffectDuration = 500; // the duration of the flow effect amination

export function highlightActorList(actorList) {
  // cancel the effect of being selected
  d3.selectAll("rect.selected")
    .attr("stroke", 'none')
    .classed("selected", false);

  d3.selectAll(`path.selected.outgoing-link-bg`)
    .attr("stroke", function () { return d3.select(this).attr("data-color") });

  d3.selectAll("path.selected.outgoing-link")
    .attr("stroke", function () { return d3.select(this).attr("data-color") });

  d3.selectAll("path.selected")
    .on("start", null)
    .classed("selected", false);

  // apply the effect of being selected
  for (let id of actorList) {
    // highlight actor box
    d3.selectAll(`rect.actor-${id}`)
      .attr("stroke", highLightColor)
      .classed("selected", true);

    // apply moving effect on internal links
    d3.selectAll(`path.actor-${id}.interal-link`)
      .classed("selected", true)
      .attr("stroke-dashoffset", "0")
      .transition()
      .duration(linkFlowEffectDuration)
      .ease(d3.easeLinear)
      .on("start", function repeat() {
        if (!d3.select(this).classed("selected")) {
          return;
        }
        d3.active(this)
          .attr("stroke-dashoffset", "20")
          .transition()
          .on("start", function () {
            d3.select(this).attr("stroke-dashoffset", "0");
            repeat.bind(this)();
          })
      });

    // apply moving effect on outgoing links of actors
    d3.selectAll(`path.actor-${id}.outgoing-link`)
      .classed("selected", true)
      .attr("data-color", function () { return d3.select(this).attr("stroke") })
      .attr("stroke", "white")
      .attr("stroke-dasharray", "20, 20")
      .attr("stroke-dashoffset", "0")
      .transition()
      .duration(linkFlowEffectDuration)
      .ease(d3.easeLinear)
      .on("start", function repeat() {
        if (!d3.select(this).classed("selected")) {
          return;
        }
        d3.active(this)
          .attr("stroke-dashoffset", "40")
          .transition()
          .on("start", function () {
            d3.select(this).attr("stroke-dashoffset", "0");
            repeat.bind(this)();
          })
      });

    // change the background color of the outgoing link
    d3.selectAll(`path.actor-${id}.outgoing-link-bg`)
      .classed("selected", true)
      .attr("data-color", function () { return d3.select(this).attr("stroke") })
      .attr("stroke", highLightColor);
  }
}