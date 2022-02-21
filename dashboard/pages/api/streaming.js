import api from "./api"

export async function getActors(){
  return (await api.get("/api/actors"));
}

export async function getFragments(){
  return await api.get("/api/fragments");
}

export async function getMaterializedViews(){
  return await api.get("/api/materialized_views");
}