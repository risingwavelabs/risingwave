import * as d3 from "d3";
import createView from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';
import SearchIcon from '@mui/icons-material/Search';
import { Tooltip, FormControl, Select, MenuItem, InputLabel, FormHelperText, Input, InputAdornment, IconButton, Autocomplete, TextField } from '@mui/material';
import { highlightActorList } from "../lib/streamPlan/chartEffect";

const width = 1000;
const height = 1000;
const orginalZoom = new d3.ZoomTransform(0.5, 0, 0);
let zoom;
let svg;
let view;

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

const ToolBoxTitle = styled('div')(() => ({
  fontSize: "15px",
  fontWeight: 700
}));


export default function StreamingView(props) {
  const data = props.data || [];
  const actorList = data.map(x => x.node);

  const [nodeJson, setNodeJson] = useState("");
  const [showInfoPane, setShowInfoPane] = useState(false);
  const [selectedActor, setSelectedActor] = useState("Show All");
  const [searchType, setSearchType] = useState("Actor");
  const [searchContent, setSearchContent] = useState("");
  const [mvTableIdToActorList, setMvTableIdToActorList] = useState(null);
  const [mviewTableList, setMviewTableList] = useState([]);

  const d3Container = useRef(null);

  const exprNode = (actorNode) => (({ input, ...o }) => o)(actorNode)

  const locateTo = (selector) => {
    let selection = d3.select(selector);
    if (!selection.empty()) {
      console.log(selection.attr("x"), selection.attr("y"));
      svg.call(zoom).call(zoom.transform, new d3.ZoomTransform(1, -selection.attr("x"), -selection.attr("y") + height / 2));
    }
  }

  const locateSearchPosition = () => {
    let type = searchType === "Operator" ? "Node" : searchType;
    type = type.toLocaleLowerCase();

    if (type === "actor") {
      locateTo(`rect.${type}-${searchContent}`);
    }
  }

  const onNodeClick = (e, node) => {
    setShowInfoPane(true);
    setNodeJson(node.dispatcherType
      ? JSON.stringify({ dispatcher: { type: node.dispatcherType }, downstreamActorId: node.downstreamActorId }, null, 2)
      : JSON.stringify(exprNode(node.nodeProto), null, 2));
  };

  const onActorSelect = (e) => {
    setSelectedActor(e.target.value);
  }

  const onSearchTypeChange = (e) => {
    setSearchType(e.target.value);
  }

  const onSearchButtonClick = (e) => {
    locateSearchPosition();
  }

  const onSearchBoxChange = (e) => {
    setSearchContent(e.target.value);
  }

  const onSelectMvChange = (e, v) => {
    if (v === null) {
      return;
    }
    let actorIdList = mvTableIdToActorList.get(v.tableId) || [];
    if (actorIdList.length !== 0) {
      highlightActorList(actorIdList);
      locateTo(`rect.actor-${actorIdList[0]}`);
    }
  }

  useEffect(() => {
    locateSearchPosition();
  }, [searchType, searchContent]);

  useEffect(() => {
    if (mvTableIdToActorList !== null) {
      let tmp = [];
      for (let [tableId, _] of mvTableIdToActorList.entries()) {
        tmp.push({ label: "mvtable " + tableId, tableId: tableId });
      }
      tmp.sort(x => x.label);
      setMviewTableList(tmp);
    }
  }, [mvTableIdToActorList])

  useEffect(() => {
    if (d3Container.current) {
      svg = d3
        .select(d3Container.current)
        .attr("viewBox", [0, 0, width, height]);

      const g = svg.append("g").attr("class", "top");
      view = createView(g, data, onNodeClick, selectedActor === "Show All" ? null : selectedActor);

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

      setMvTableIdToActorList(view.getMvTableIdToActorList());

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
      <SvgBoxCover className="noselect" style={{ display: "flex", flexDirection: "column" }}>
        {/* Select actor */}
        <ToolBoxTitle>
          Select a worker node
        </ToolBoxTitle>
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

        {/* Search box */}
        <ToolBoxTitle>
          Search
        </ToolBoxTitle>
        <div style={{ display: "flex", flexDirection: "row", alignItems: "center" }}>
          <FormControl sx={{ m: 1, minWidth: 120 }}>
            <InputLabel>Type</InputLabel>
            <Select
              value={searchType}
              label="Type"
              onChange={onSearchTypeChange}
            >
              <MenuItem value="Actor">Actor</MenuItem>
            </Select>
          </FormControl>
          <Input
            sx={{ m: 1, width: 150 }}
            label="Search" variant="standard"
            onChange={onSearchBoxChange}
            value={searchContent}
            endAdornment={
              <InputAdornment position="end">
                <IconButton
                  aria-label="toggle password visibility"
                  onClick={onSearchButtonClick}
                >
                  <SearchIcon />
                </IconButton>
              </InputAdornment>
            } />
        </div>

        {/* Search box */}
        <ToolBoxTitle>
          Filter materialized view
        </ToolBoxTitle>
        <div style={{ display: "flex", flexDirection: "row", alignItems: "center" }}>
          <FormControl sx={{ m: 1, minWidth: 300 }}>
            <Autocomplete
              disablePortal
              options={mviewTableList || []}
              onChange={onSelectMvChange}
              renderInput={(param) => <TextField {...param} label="Materialized View" />}
            />
          </FormControl>
        </div>
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