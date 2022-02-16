import Layout from '../components/Layout';
import StreamingView from '../components/StreamingView';
import NoData from '../components/NoData';
import { getActors } from './api/streaming';
import { cloneDeep } from "lodash";

export async function getStaticProps(context) {
  try {
    let actorProtoList = await getActors();
    return {
      props: {
        actorProtoList
      }
    }
  } catch (e) {
    console.error("failed to fetch data from meta node.")
    return {
      props: {
        actorProtoList: []
      }
    }
  }
}

export default function Streaming(props) {
  return (
    <>
      <Layout currentPage="streaming">
        {props.actorProtoList.length !== 0 ?
          <StreamingView
            data={props.actorProtoList}
          />
          : <NoData />}
      </Layout>
    </>
  )
}