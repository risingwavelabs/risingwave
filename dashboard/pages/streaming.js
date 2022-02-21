import Layout from '../components/Layout';
import StreamingView from '../components/StreamingView';
import NoData from '../components/NoData';
import { getActors, getFragments, getMaterializedViews } from './api/streaming';

export async function getStaticProps(context) {
  try {
    let actorProtoList = await getActors();
    let mvList = await getMaterializedViews();
    let fragments = await getFragments();
    return {
      props: {
        actorProtoList,
        mvList,
        fragments
      }
    }
  } catch (e) {
    console.error("failed to fetch data from meta node.")
    return {
      props: {
      }
    }
  }
}
 
export default function Streaming(props) {
  console.log(props.fragments);
  return (
    <>
      <Layout currentPage="streaming">
        {props.actorProtoList 
          && props.actorProtoList.length !== 0 
          && props.actorProtoList[0].actors?
          <StreamingView
            data={props.actorProtoList}
            mvList={props.mvList}
          />
          : <NoData />}
      </Layout>
    </>
  )
}