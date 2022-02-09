import config from "../../config";



class Api {

  constructor() {
    this.baseUrl = config.baseUrl.charAt(config.baseUrl.length - 1) === "/"
      ? config.baseUrl.slice(0, config.baseUrl.length)
      : config.baseUrl;
  }

  async get(url) {
    try {
      const res = await fetch(this.baseUrl + url);
      const data = await res.json();
      return data;
    } catch (e) {
      console.error(e);
    }

  }
}


export default new Api();