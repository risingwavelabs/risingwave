import createView, { computeNodeAddrToSideColor } from "../lib/streamPlan/streamChartHelper";
import { useEffect, useRef, useState } from 'react';
import styled from '@emotion/styled';
import LocationSearchingIcon from '@mui/icons-material/LocationSearching';
import SearchIcon from '@mui/icons-material/Search';
import { Tooltip, FormControl, Select, MenuItem, InputLabel, FormHelperText, Input, InputAdornment, IconButton, Autocomplete, TextField, FormControlLabel, Switch, Divider } from '@mui/material';
import { CanvasEngine } from "../lib/graaphEngine/canvasEngine";
import useWindowSize from "../hook/useWindowSize";


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

  let view;
  let engine;
  let shownActorIdList = null;


  const canvasRef = useRef(null);
  const canvasOutterBox = useRef(null);

  const exprNode = (actorNode) => (({ input, ...o }) => o)(actorNode)

  const locateTo = (selector) => {
    console.log(engine)
    engine && engine.locateTo(selector);
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
      locateTo(`actor-${shownActorIdList[0]}`)
    }
  }

  const onReset = () => {
    engine.resetCamera();
  }

  const resizeCanvas = () => {
    if (canvasOutterBox.current) {
      engine && engine.resize(canvasOutterBox.current.clientWidth, canvasOutterBox.current.clientHeight);
    }
  }

  const initGraph = () => {
    engine = new CanvasEngine("c", canvasRef.current.clientHeight, canvasRef.current.cientWidth);
    resizeCanvas();
    view = createView(engine, data, onNodeClick, selectedWorkerNode === "Show All" ? null : selectedWorkerNode, shownActorIdList);
  };

  const windowSize = useWindowSize();

  useEffect(() => {
    console.log("resize here")
    resizeCanvas();
  }, [windowSize])

  useEffect(() => {
    locateSearchPosition();
  }, [searchType, searchContent]);

  // render the full graph
  useEffect(() => {
    if (canvasRef.current && showFullGraph) {
      shownActorIdList = null;
      initGraph();

      mvTableIdToSingleViewActorList || setMvTableIdToSingleViewActorList(view.getMvTableIdToSingleViewActorList());
      mvTableIdToChainViewActorList || setMvTableIdToChainViewActorList(view.getMvTableIdToChainViewActorList());
      return () => engine.cleanGraph();
    }
  }, [selectedWorkerNode, showFullGraph]);

  // locate and render partial graph on mview query
  useEffect(() => {
    if (selectedMvTableId === null) {
      return;
    }
    shownActorIdList = (filterMode === "Chain View" ? mvTableIdToChainViewActorList : mvTableIdToSingleViewActorList)
      .get(selectedMvTableId) || [];
    if (showFullGraph) { // locate to the selected part
      shownActorIdList = (filterMode === "Chain View" ? mvTableIdToChainViewActorList : mvTableIdToSingleViewActorList)
        .get(selectedMvTableId);
    } else {
      if (canvasRef.current) {
        initGraph();
        return () => engine.cleanGraph();
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
              options={mvList.map(mv => { return { label: mv[1].tableName, tableId: mv[0] } }) || []}
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
      <div ref={canvasOutterBox} style={{ zIndex: 5, width: "100%", height: "100%", overflow: "auto" }} className="noselect">
        <canvas ref={canvasRef} id="c" width={1000} height={1000} />
      </div>
    </SvgBox >
  )
}