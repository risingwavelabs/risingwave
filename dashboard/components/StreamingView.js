import * as d3 from "d3";
import createView from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';

import { Tooltip, FormControl, Select, MenuItem, InputLabel, FormHelperText } from '@mui/material';

const orginalZoom = new d3.ZoomTransform(0.5, 0, 0);
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
  const data = props.data || [];
  const actorList = data.map(x => x.node);

  const [nodeJson, setNodeJson] = useState("");
  const [showInfoPane, setShowInfoPane] = useState(false);
  const [selectedActor, setSelectedActor] = useState("Show All");

  const d3Container = useRef(null);

  const exprNode = (actorNode) => (({ input, ...o }) => o)(actorNode)

  const onNodeClick = (e, node) => {
    setShowInfoPane(true);
    setNodeJson(node.dispatcherType
      ? JSON.stringify({ dispatcher: { type: node.dispatcherType }, downstreamActorId: node.downstreamActorId }, null, 2)
      : JSON.stringify(exprNode(node.nodeProto), null, 2));
  };

  const onActorSelect = (e) => {
    setSelectedActor(e.target.value);
  }

  useEffect(() => {
    if (d3Container.current) {
      const width = 1000;
      const height = 1000;

      svg = d3
        .select(d3Container.current)
        .attr("viewBox", [-width / 6, -height / 4, width, height]);

      const g = svg.append("g").attr("class", "top");
      createView(g, data, onNodeClick, selectedActor === "Show All" ? null : selectedActor);

      let transform;
      // Deal with zooming event
      zoom = d3.zoom().on("zoom", e => {
        transform = e.transform;
        g.attr("transform", e.transform);
      });

      svg.call(zoom)
        .call(zoom.transform, orginalZoom)
        .on("pointermove", event => {
          transform.invert(d3.pointer(event));
        });

      return () => d3.select(d3Container.current).selectAll("*").remove();
    }
  }, [selectedActor]);

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
      <SvgBoxCover className="noselect">
        <FormControl sx={{ m: 1, minWidth: 300 }}>
          <InputLabel>Actor</InputLabel>
          <Select
            value={selectedActor || "Show All"}
            label="Actor"
            onChange={onActorSelect}
          >
            <MenuItem value="Show All" key="all">
              Show All
            </MenuItem>
            {actorList.map((x, index) => {
              let v = `${x.type} ${x.host.host}:${x.host.port}`;
              return (
                <MenuItem value={x} key={index}>
                  {x.type}&nbsp; <span style={{ fontWeight: 700 }}>{x.host.host + ":" + x.host.port}</span>
                </MenuItem>
              )
            })}
          </Select>
          <FormHelperText>Select an Actor</FormHelperText>
        </FormControl>
      </SvgBoxCover>
      <SvgBoxCover style={{ right: "10px", bottom: "10px", cursor: "pointer" }}>
        <Tooltip title="Reset">
          <div onClick={() => { svg.call(zoom.transform, orginalZoom) }}>
            <LocationSearchingIcon color="action" />
          </div>
        </Tooltip>
      </SvgBoxCover>
      <div style={{ zIndex: 5 }} className="noselect">
        <svg ref={d3Container}
          width="100%"
          height={600}
        />
      </div>
    </SvgBox >
  )
}