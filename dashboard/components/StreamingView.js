import * as d3 from "d3";
import createView, { computeNodeAddrToSideColor } from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';
import FmdGoodIcon from '@mui/icons-material/FmdGood';
import SearchIcon from '@mui/icons-material/Search';
import { Tooltip, FormControl, Select, MenuItem, InputLabel, FormHelperText, Input, InputAdornment, IconButton, Autocomplete, TextField, FormControlLabel, Switch, Divider } from '@mui/material';

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
  const mvList = props.mvList || [];
  const actorList = data.map(x => x.node);

  const [nodeJson, setNodeJson] = useState("");
  const [showInfoPane, setShowInfoPane] = useState(false);
  const [selectedWorkerNode, setSelectedWorkerNode] = useState("Show All");
  const [searchType, setSearchType] = useState("Actor");
  const [searchContent, setSearchContent] = useState("");
  const [mvTableIdToSingleViewActorList, setMvTableIdToSingleViewActorList] = useState(null);
  const [mvTableIdToChainViewActorList, setMvTableIdToChainViewActorList] = useState(null);
  const [filterMode, setFilterMode] = useState("Chain View");
  const [selectedMvTableId, setSelectedMvTableId] = useState(null);
  const [showFullGraph, setShowFullGraph] = useState(true);

  const d3Container = useRef(null);

  // let tmp = [];
  // for(let mv of mvList){
  //   tmp.push({
  //     label: mv[1].tableName, tableId: mv[0]
  //   })
  // }

  const exprNode = (actorNode) => (({ input, ...o }) => o)(actorNode)

  const locateTo = (selector) => {
    let selection = d3.select(selector);
    if (!selection.empty()) {
      svg.call(zoom).call(zoom.transform, new d3.ZoomTransform(0.7, - 0.7 * selection.attr("x"), 0.7 *(-selection.attr("y") + height / 2) ));
    }
  }

  const locateSearchPosition = () => {
    let type = searchType === "Operator" ? "Node" : searchType;
    type = type.toLocaleLowerCase();

    if (type === "actor") {
      locateTo(`rect.${type}-${searchContent}`);
    }

    if (type === "fragment"){
      locateTo(`rect.${type}-${searchContent}`);
    }
  }

  const onNodeClick = (e, node) => {
    setShowInfoPane(true);
    setNodeJson(node.dispatcherType
      ? JSON.stringify({ dispatcher: { type: node.dispatcherType }, downstreamActorId: node.downstreamActorId }, null, 2)
      : JSON.stringify(exprNode(node.nodeProto), null, 2));
  };

  const onWorkerNodeSelect = (e) => {
    setSelectedWorkerNode(e.target.value);
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

  const onFullGraphSwitchChange = (e, v) => {
    setShowFullGraph(v);
  }

  const locateToCurrentMviewActor = () => {
    let shownActorIdList = (filterMode === "Chain View" ? mvTableIdToChainViewActorList : mvTableIdToSingleViewActorList)
      .get(selectedMvTableId) || [];
    if (shownActorIdList.length !== 0) {
      locateTo(locateTo(`rect.actor-${shownActorIdList[0]}`));
    }
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

  // useEffect(() => {
  //   if (mvTableIdToSingleViewActorList !== null) {
  //     let tmp = [];
  //     for (let [tableId, _] of mvTableIdToSingleViewActorList.entries()) {
  //       tmp.push({ label: "mvtable " + tableId, tableId: tableId });
  //     }
  //     tmp.sort(x => x.label);
  //     setMviewTableList(tmp);
  //   }
  // }, [mvTableIdToSingleViewActorList])

  // render the full graph
  useEffect(() => {
    if (d3Container.current && showFullGraph) {
      createSvg((g) => {
        view = createView(g, data, onNodeClick, selectedWorkerNode === "Show All" ? null : selectedWorkerNode, null);
      })
      // assign once.
      mvTableIdToSingleViewActorList || setMvTableIdToSingleViewActorList(view.getMvTableIdToSingleViewActorList());
      mvTableIdToChainViewActorList || setMvTableIdToChainViewActorList(view.getMvTableIdToChainViewActorList());

      return () => d3.select(d3Container.current).selectAll("*").remove();
    }
  }, [selectedWorkerNode, showFullGraph]);

  // locate and render partial graph on mview query
  useEffect(() => {
    if (selectedMvTableId === null) {
      view.highlightByActorIds([]);
      return;
    }
    let shownActorIdList = (filterMode === "Chain View" ? mvTableIdToChainViewActorList : mvTableIdToSingleViewActorList)
      .get(selectedMvTableId) || [];
    if (showFullGraph) { // locate to the selected part
      let shownActorIdList = (filterMode === "Chain View" ? mvTableIdToChainViewActorList : mvTableIdToSingleViewActorList)
        .get(selectedMvTableId);
      view.highlightByActorIds(shownActorIdList);
    } else {
      if (d3Container.current) {
        createSvg((g) => {
          view = createView(g, data, onNodeClick, selectedWorkerNode === "Show All" ? null : selectedWorkerNode, shownActorIdList);
        });
      }
    }
    locateToCurrentMviewActor();
  }, [selectedWorkerNode, filterMode, selectedMvTableId, showFullGraph])

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
            value={selectedWorkerNode || "Show All"}
            label="Actor"
            onChange={onWorkerNodeSelect}
          >
            <MenuItem value="Show All" key="all">
              Show All
            </MenuItem>
            {actorList.map((x, index) => {
              let v = `${x.type} ${x.host.host}:${x.host.port}`;
              return (
                <MenuItem value={x} key={index} sx={{display: "flex", flexDirection: "row", alignItems: "center"}}>
                  {x.type}&nbsp; <span style={{ fontWeight: 700 }}>{x.host.host + ":" + x.host.port}</span>
                  <div style={{
                    margin: 5,
                    height: 10, width: 10, borderRadius: 5,
                    backgroundColor: computeNodeAddrToSideColor(x.host.host + ":" + x.host.port)}} 
                  />
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
              <MenuItem value="Fragment">Fragment</MenuItem>
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
        <div style={{ display: "flex", flexDirection: "column" }}>
          <FormControl sx={{ m: 1, width: 300 }}>
            <div style={{ display: "flex", flexDirection: "row", alignItems: "center", marginBottom: "10px" }}>
              <div>
                <InputLabel>Mode</InputLabel>
                <Select
                  sx={{ width: 140 }}
                  value={filterMode}
                  label="Mode"
                  onChange={onFilterModeChange}
                >
                  <MenuItem value="Single View">Single View</MenuItem>
                  <MenuItem value="Chain View">Chain View</MenuItem>
                </Select>
              </div>
              <div style={{ marginLeft: "20px" }}>Full Graph</div>
              <Switch
                defaultChecked
                value={showFullGraph}
                onChange={onFullGraphSwitchChange}
              />
            </div>
            <Autocomplete
              isOptionEqualToValue={(option, value) => {return option.tableId === value.tableId}}
              disablePortal
              options={mvList.map(mv => {return {label: mv[1].tableName, tableId: mv[0]}}) || []}
              onChange={onSelectMvChange}
              renderInput={(param) => <TextField {...param} label="Materialized View" />}
            />
          </FormControl>
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