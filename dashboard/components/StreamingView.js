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
import createView, { computeNodeAddrToSideColor } from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';
import CircularProgress from '@mui/material/CircularProgress';
import SearchIcon from '@mui/icons-material/Search';
import RefreshIcon from '@mui/icons-material/Refresh';
import { Stack, Tabs, Tab, Button } from "@mui/material";
import { Tooltip, FormControl, Select, MenuItem, InputLabel, FormHelperText, Input, InputAdornment, IconButton, Autocomplete, TextField, FormControlLabel, Switch, Divider } from '@mui/material';
import { CanvasEngine } from "../lib/graaphEngine/canvasEngine";
import useWindowSize from "../hook/useWindowSize";
import { Close } from "@mui/icons-material";
import SyntaxHighlighter from "react-syntax-highlighter";
import { stackoverflowDark } from 'react-syntax-highlighter/dist/cjs/styles/hljs';


const SvgBox = styled('div')(() => ({
  padding: "10px",
  borderRadius: "20px",
  boxShadow: "5px 5px 10px #ebebeb, -5px -5px 10px #ffffff",
  position: "relative",
  marginBottom: "100px",
  height: "100%",
  width: "100%"
}));

const SvgBoxCover = styled('div')(() => ({
  position: "absolute",
  zIndex: "6"
}));

const ToolBoxTitle = styled('div')(() => ({
  fontSize: "15px",
  fontWeight: 700
}));

const PopupBox = styled("div")({
  width: "100%",
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  backgroundColor: "white",
  borderRadius: "20px",
});

const PopupBoxHeader = styled("div")({
  padding: "10px",
  display: "flex",
  flexDirection: "row",
  justifyContent: "end",
  alignItems: "center",
  backgroundColor: "#1976D2",
  borderTopRightRadius: "20px",
  borderTopLeftRadius: "20px",
  height: "50px"
});

const generateMessageTraceLink = (actorId) => {
  return `http://localhost:16680/search?service=compute&tags=%7B%22actor_id%22%3A%22${actorId}%22%2C%22msg%22%3A%22chunk%22%7D`;
}

const generateEpochTraceLink = (actorId) => {
  return `http://localhost:16680/search?service=compute&tags=%7B%22actor_id%22%3A%22${actorId}%22%2C%22epoch%22%3A%22-1%22%7D`;
}

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
  const [refreshing, setRefreshing] = useState(false);
  const [tabValue, setTabValue] = useState(0);
  const [actor, setActor] = useState(null);


  const canvasRef = useRef(null);
  const canvasOuterBox = useRef(null);
  const engineRef = useRef(null);
  const viewRef = useRef(null);

  const setEngine = (e) => {
    engineRef.current = e;
  }

  const getEngine = () => {
    return engineRef.current;
  }

  const setView = (v) => {
    viewRef.current = v;
  }

  const getView = () => {
    return viewRef.current;
  }

  const exprNode = (actorNode) => (({ input, ...o }) => o)(actorNode)

  const locateTo = (selector) => {
    getEngine() && getEngine().locateTo(selector);
  }

  const onTabChange = (_, v) => {
    setTabValue(v);
  }

  const locateSearchPosition = () => {
    let type = searchType === "Operator" ? "Node" : searchType;
    type = type.toLocaleLowerCase();

    if (type === "actor") {
      locateTo(`${type}-${searchContent}`);
    }

    if (type === "fragment") {
      locateTo(`${type}-${searchContent}`);
    }
  }

  const onNodeClick = (e, node, actor) => {
    setActor(actor);
    setShowInfoPane(true);
    setNodeJson(node.dispatcherType
      ? JSON.stringify({ dispatcher: { type: node.dispatcherType }, downstreamActorId: node.downstreamActorId }, null, 2)
      : JSON.stringify(exprNode(node.nodeProto), null, 2));
  };

  const onActorClick = (e, actor) => {
    setActor(actor);
    setShowInfoPane(true);
    setNodeJson("Click a node to show its raw json");
  }


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

  const locateToCurrentMviewActor = (actorIdList) => {
    if (actorIdList.length !== 0) {
      locateTo(`actor-${actorIdList[0]}`)
    }
  }

  const onReset = () => {
    getEngine().resetCamera();
  }

  const onRefresh = async () => {
    window.location.reload(true);
  }

  const resizeCanvas = () => {
    if (canvasOuterBox.current) {
      getEngine() && getEngine().resize(canvasOuterBox.current.clientWidth, canvasOuterBox.current.clientHeight);
    }
  }

  const initGraph = (shownActorIdList) => {
    let newEngine = new CanvasEngine("c", canvasRef.current.clientHeight, canvasRef.current.clientWidth);
    setEngine(newEngine);
    resizeCanvas();
    let newView = createView(newEngine, data, onNodeClick, onActorClick, selectedWorkerNode === "Show All" ? null : selectedWorkerNode, shownActorIdList);
    setView(newView);
  };

  const windowSize = useWindowSize();

  useEffect(() => {
    resizeCanvas();
  }, [windowSize])

  useEffect(() => {
    locateSearchPosition();
  }, [searchType, searchContent]);

  // render the full graph
  useEffect(() => {
    if (canvasRef.current && showFullGraph) {
      initGraph(null);

      mvTableIdToSingleViewActorList || setMvTableIdToSingleViewActorList(getView().getMvTableIdToSingleViewActorList());
      mvTableIdToChainViewActorList || setMvTableIdToChainViewActorList(getView().getMvTableIdToChainViewActorList());
      return () => {
        getEngine().cleanGraph();
      };
    }
  }, [selectedWorkerNode, showFullGraph]);

  // locate and render partial graph on mview query
  useEffect(() => {
    if (selectedMvTableId === null) {
      return;
    }
    let shownActorIdList = (filterMode === "Chain View" ? mvTableIdToChainViewActorList : mvTableIdToSingleViewActorList)
      .get(selectedMvTableId) || [];
    if (!showFullGraph) { // rerender graph if it is a partial graph
      if (canvasRef.current) {
        initGraph(shownActorIdList);
        return () => {
          getEngine().cleanGraph();
        };
      }
    }
    locateToCurrentMviewActor(shownActorIdList);
  }, [selectedWorkerNode, filterMode, selectedMvTableId, showFullGraph])

  return (
    <SvgBox>
      <SvgBoxCover style={{ right: "10px", top: "10px", width: "500px" }}>
        <PopupBox style={{
          display: showInfoPane ? "block" : "none",
          height: canvasOuterBox && canvasOuterBox.current ? canvasOuterBox.current.clientHeight - 100 : 500
        }}>
          <PopupBoxHeader>
            <IconButton onClick={() => setShowInfoPane(false)}>
              <Close sx={{ color: "white" }} />
            </IconButton>
          </PopupBoxHeader>
          <Tabs value={tabValue} onChange={onTabChange} aria-label="basic tabs example">
            <Tab label="Info" id={0} />
            <Tab label="Raw JSON" id={1} />
          </Tabs>
          <div style={{
            display: tabValue === 0 ? "flex" : "none",
            flexDirection: "column",
            padding: "10px",
            width: "100%",
            height: "calc(100% - 160px)",
            overflow: "auto"
          }}>
            {actor && actor.representedActorList.map((a, i) =>
              <Stack key={i} direction="column" justifyContent="center" spacing={1} style={{ width: "100%", marginBottom: "30px" }}>
                <div style={{ fontSize: "15px", color: "#1976D2" }}>
                  Actor {a.actorId}
                </div>
                <a target="_blank" rel="noopener noreferrer" href={generateMessageTraceLink(a.actorId)}>
                  <Button variant="outlined">
                    Trace Message of Actor #{a.actorId}
                  </Button>
                </a>
                <a target="_blank" rel="noopener noreferrer" href={generateEpochTraceLink(a.actorId)}>
                  <Button variant="outlined">
                    Trace Epoch "-1" of Actor #{a.actorId}
                  </Button>
                </a>
              </Stack>
            )}
          </div>
          <div style={{ display: tabValue === 1 ? "block" : "none", height: "calc(100% - 160px)", overflow: "auto" }}>
            <SyntaxHighlighter language="json" style={stackoverflowDark} wrapLines={true} showLineNumbers={true}>
              {nodeJson}
            </SyntaxHighlighter>
          </div>

        </PopupBox>
      </SvgBoxCover>
      <SvgBoxCover className="noselect" style={{ display: "flex", flexDirection: "column" }}>
        {/* Select actor */}
        <ToolBoxTitle>
          Select a worker node
        </ToolBoxTitle>
        <FormControl sx={{ m: 1, minWidth: 300 }}>
          <InputLabel>Worker Node</InputLabel>
          <Select
            value={selectedWorkerNode || "Show All"}
            label="Woker Node"
            onChange={onWorkerNodeSelect}
          >
            <MenuItem value="Show All" key="all">
              Show All
            </MenuItem>
            {actorList.map((x, index) => {
              return (
                <MenuItem value={x} key={index} sx={{ display: "flex", flexDirection: "row", alignItems: "center" }}>
                  {x.type}&nbsp; <span style={{ fontWeight: 700 }}>{x.host.host + ":" + x.host.port}</span>
                  <div style={{
                    margin: 5,
                    height: 10, width: 10, borderRadius: 5,
                    backgroundColor: computeNodeAddrToSideColor(x.host.host + ":" + x.host.port)
                  }}
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
              isOptionEqualToValue={(option, value) => { return option.tableId === value.tableId }}
              disablePortal
              options={mvList.map(mv => { return { label: mv.name, tableId: mv.id } }) || []}
              onChange={onSelectMvChange}
              renderInput={(param) => <TextField {...param} label="Materialized View" />}
            />
          </FormControl>
        </div>
      </SvgBoxCover>

      <SvgBoxCover style={{ right: "10px", bottom: "10px", cursor: "pointer" }}>
        <Stack direction="row" spacing={2}>
          <Tooltip title="Reset">
            <div onClick={() => onReset()}>
              <LocationSearchingIcon color="action" />
            </div>
          </Tooltip>

          <Tooltip title="refresh">
            {!refreshing
              ? <div onClick={() => onRefresh()}>
                <RefreshIcon color="action" />
              </div>
              : <CircularProgress />}
          </Tooltip>
        </Stack>


      </SvgBoxCover>
      <div ref={canvasOuterBox} style={{ zIndex: 5, width: "100%", height: "100%", overflow: "auto" }} className="noselect">
        <canvas ref={canvasRef} id="c" width={1000} height={1000} style={{ cursor: "pointer" }} />
      </div>
    </SvgBox >
  )
}