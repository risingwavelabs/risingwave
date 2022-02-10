import api from "./api";

export async function getClusterInfoFrontend(){
  const res = await api.get("/api/clusters/0");
  return res;
}

export async function getClusterInfoComputeNode(){
  const res = await api.get("/api/clusters/1");
  return res;
}