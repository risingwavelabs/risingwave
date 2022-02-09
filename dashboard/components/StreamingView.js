import * as d3 from "d3";
import { drawManyFlow } from "../lib/streamChartHelper";
import { useEffect, useRef } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';

import { Tooltip } from '@mui/material';

const orginalZoom = new d3.ZoomTransform(0.3, 0, 0);

const SvgBox = styled('div')(() => ({
  padding: "10px",
  borderRadius: "20px",
  boxShadow: "5px 5px 10px #ebebeb, -5px -5px 10px #ffffff",
  position: "relative"
}));

const SvgBoxCover = styled('div')(() => ({
  position: "absolute",
  zIndex: "6"
}));

export default function StreamingView(props) {

  const node = props.node;
  const actorProto = props.actorProto;

  const d3Container = useRef(null);

  let zoom;
  let svg;

  useEffect(() => {
    if (d3Container.current) {
      const width = 1000;
      const height = 1000;

      svg = d3
        .select(d3Container.current)
        .attr("viewBox", [-width / 6, -height / 4, width, height]);

      const g = svg.append("g");

      drawManyFlow({
        g: g,
        actorProto: actorProto
      });

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
  }, [d3Container.current])

  return (
    <SvgBox>
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
    </SvgBox>
  )
}