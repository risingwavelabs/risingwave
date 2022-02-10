import React from 'react';
import { useState } from 'react';

import Layout from '../components/Layout';
import NoData from '../components/NoData';
import { getClusterInfoFrontend, getClusterInfoComputeNode } from "./api/cluster";

import Box from '@mui/material/Box';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import StatusLamp from '../components/StatusLamp';

const NodeTable = (props) => {
  return (
    <Box sx={{ width: "100%", maxWidth: 1000 }}>
      {props.data.length !== 0 ? <TableContainer component={Paper}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>ID</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Host</TableCell>
              <TableCell>Post</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {props.data.map((row, i) => (
              <TableRow key={i}>
                <TableCell>{row.id}</TableCell>
                <TableCell sx={{ color: "green" }}>
                  <div style={{display: "flex", flexDirection: "row", alignItems: "center"}}>
                    <StatusLamp color="green" />Running
                  </div>
                </TableCell>
                <TableCell>{row.host.host}</TableCell>
                <TableCell>{row.host.port}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
        : <NoData />}
    </Box>
  )
}

export async function getStaticProps(context) {
  let frontendList = await getClusterInfoFrontend();
  let computeNodeList = await getClusterInfoComputeNode();
  return {
    props: {
      frontendList,
      computeNodeList
    }
  }
}

export default function Cluster(props) {

  const [frontendList, setFrontendList] = useState(props.frontendList || []);
  const [computeNodeList, setComputeNodeList] = useState(props.computeNodeList || [])

  return (
    <>
      <Layout currentPage="cluster">
        <p>Frontend</p>
        <NodeTable data={frontendList} />
        <p>Compute Node</p>
        <NodeTable data={computeNodeList} />
      </Layout>
    </>
  )
}