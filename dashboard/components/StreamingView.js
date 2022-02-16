import * as d3 from "d3";
import createView from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';
import SearchIcon from '@mui/icons-material/Search';
import { Tooltip, FormControl, Select, MenuItem, InputLabel, FormHelperText, Input, InputAdornment, IconButton, Autocomplete, TextField } from '@mui/material';

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
  const [mvTableIdToSingleViewActorList, setMvTableIdToSingleViewActorList] = useState(null);
  const [mvTableIdToChainViewActorList, setMvTableIdToChainViewActorList] = useState(null);
  const [mviewTableList, setMviewTableList] = useState([]);
  const [filterMode, setFilterMode] = useState("Full Graph");
  const [selectedMvTableId, setSelectedMvTableId] = useState(null);

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
    setSelectedMvTableId(v === null ? null : v.tableId);
  }

  const onFilterModeChange = (e) => {
    setFilterMode(e.target.value);
  }

  const onReset = () => {
    svg.call(zoom.transform, orginalZoom);
    view.highlightByActorIds([]);
  }

  const createSvg = (callback) => {
    d3.select(d3Container.current).selectAll("*").remove();

    svg = d3
      .select(d3Container.current)
      .attr("viewBox", [0, 0, width, height]);

    const g = svg.append("g").attr("class", "top");
    callback && callback(g);

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
  }

  useEffect(() => {
    locateSearchPosition();
  }, [searchType, searchContent]);

  useEffect(() => {
    if (mvTableIdToSingleViewActorList !== null) {
      let tmp = [];
      for (let [tableId, _] of mvTableIdToSingleViewActorList.entries()) {
        tmp.push({ label: "mvtable " + tableId, tableId: tableId });
      }
      tmp.sort(x => x.label);
      setMviewTableList(tmp);
    }
  }, [mvTableIdToSingleViewActorList])

  // render the full graph
  useEffect(() => {
    if (d3Container.current && filterMode === "Full Graph") {
      createSvg((g) => {
        view = createView(g, data, onNodeClick, selectedActor === "Show All" ? null : selectedActor, null);
      })
      // assign once.
      mvTableIdToSingleViewActorList || setMvTableIdToSingleViewActorList(view.getMvTableIdToSingleViewActorList());
      mvTableIdToChainViewActorList || setMvTableIdToChainViewActorList(view.getMvTableIdToChainViewActorList());

      return () => d3.select(d3Container.current).selectAll("*").remove();
    }
  }, [selectedActor, filterMode]);

  // render the partial graph
  useEffect(() => {
    if (selectedMvTableId === null) {
      return;
    }
    if (filterMode === "Full Graph") {
      let actorIdList = mvTableIdToSingleViewActorList.get(selectedMvTableId) || [];
      if (actorIdList.length !== 0) {
        view.highlightByActorIds(actorIdList);
        locateTo(`rect.actor-${actorIdList[0]}`);
      }
    } else {
      if (d3Container.current) {
        let shownActorIdlist = filterMode === "Single View"
          ? mvTableIdToSingleViewActorList.get(selectedMvTableId)
          : mvTableIdToChainViewActorList.get(selectedMvTableId);
        createSvg((g) => {
          view = createView(g, data, onNodeClick, selectedActor === "Show All" ? null : selectedActor, shownActorIdlist);
        });
        locateTo(`rect.actor-${shownActorIdlist[0]}`);
      }
    }
  }, [filterMode, selectedMvTableId])

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
          <FormControl sx={{ m: 1, width: 140 }}>
            <InputLabel>Mode</InputLabel>
            <Select
              value={filterMode}
              label="Mode"
              onChange={onFilterModeChange}
            >
              <MenuItem value="Full Graph">Full Graph</MenuItem>
              <MenuItem value="Single View">Single View</MenuItem>
              <MenuItem value="Chain View">Chain View</MenuItem>
            </Select>
          </FormControl>
          <Autocomplete
            sx={{ minWidth: 200 }}
            disablePortal
            options={mviewTableList || []}
            onChange={onSelectMvChange}
            renderInput={(param) => <TextField {...param} label="Materialized View" />}
          />
        </div>
      </SvgBoxCover>

      <SvgBoxCover style={{ right: "10px", bottom: "10px", cursor: "pointer" }}>

        <Tooltip title="Reset">
          <div onClick={() => onReset()}>
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