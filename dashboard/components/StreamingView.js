import * as d3 from "d3";
import createView from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';

import { Tooltip } from '@mui/material';

const orginalZoom = new d3.ZoomTransform(0.3, 0, 0);
let zoom;
let svg;

const SvgBox = styled('div')(() => ({
  padding: "10px",
  borderRadius: "20px",
  boxShadow: "5px 5px 10px #ebebeb, -5px -5px 10px #ffffff",
  position: "relative",
  marginBottom: "100px"
}));

const SvgBoxCover = styled('div')(() => ({
  position: "absolute",
  zIndex: "6"
}));


export default function StreamingView(props) {
  console.log("called.")
  const node = props.node;
  const actorProto = props.actorProto;
  const [nodeJson, setNodeJson] = useState("");
  const [showInfoPane, setShowInfoPane] = useState(false);
  const d3Container = useRef(null);

  const onNodeClick = (e, node) => {
    setShowInfoPane(true);
    setNodeJson(node.dispatcherType
      ? JSON.stringify({ dispatcher: { type: node.dispatcherType }, downstreamActorId: node.downstreamActorId }, null, 2)
      : JSON.stringify(node.nodeProto, null, 2));
  };

  useEffect(() => {
    if (d3Container.current && (svg === undefined)) {
      const width = 1000;
      const height = 1000;

      svg = d3
        .select(d3Container.current)
        .attr("viewBox", [-width / 6, -height / 4, width, height]);

      const g = svg.append("g").attr("class", "top");
      createView(g, actorProto, onNodeClick);

      let transform;
      // Deal with zooming event
      zoom = d3.zoom().on("zoom", e => {
        transform = e.transform;
        g.attr("transform", e.transform);
      })

      svg.call(zoom)
        .call(zoom.transform, orginalZoom)
        .on("pointermove", event => {
          transform.invert(d3.pointer(event));
        });
      ;
    }
  }, []);

  return (
    <SvgBox>
      <SvgBoxCover style={{ right: "10px", top: "10px", width: "300px" }}>
        <div style={{
          height: "560px",
          display: showInfoPane ? "block" : "none", padding: "20px",
          backgroundColor: "snow", whiteSpace: "pre", lineHeight: "100%",
          fontSize: "13px", overflow: "scroll"
        }}>
          {nodeJson}
        </div>

      </SvgBoxCover>
      <SvgBoxCover>
        <div>
          Actors {node ? (node.host.host + ":" + node.host.port) : ""}
        </div>
      </SvgBoxCover>
      <SvgBoxCover style={{ right: "10px", bottom: "10px", cursor: "pointer" }}>
        <Tooltip title="Reset">
          <div onClick={() => { svg.call(zoom.transform, orginalZoom) }}>
            <LocationSearchingIcon color="action" />
          </div>
        </Tooltip>
      </SvgBoxCover>
      <div style={{ zIndex: 5 }}>
        <svg ref={d3Container}
          width="100%"
          height={600}
        />
      </div>
    </SvgBox >
  )
}