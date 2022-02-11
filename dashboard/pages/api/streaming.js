import api from "./api"

export async function getActors(){
  return (await api.get("/api/actors"));
}