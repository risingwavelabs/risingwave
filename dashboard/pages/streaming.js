import Layout from '../components/Layout';
import StreamingView from '../components/StreamingView';
import NoData from '../components/NoData';
import { getActors } from './api/streaming';

export async function getStaticProps(context) {
  try {
    let actorProtoList = await getActors();
    console.log(actorProtoList);
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
        {props.actorProtoLis 
          && props.actorProtoList.length !== 0 
          && props.actorProtoLis[0].actors?
          <StreamingView
            data={props.actorProtoList}
          />
          : <NoData />}
      </Layout>
    </>
  )
}