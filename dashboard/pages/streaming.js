import StreamingView from '../components/StreamingView';
import NoData from '../components/NoData';
import Message from '../components/Message';
import { getActors, getFragments, getMaterializedViews } from './api/streaming';
import { useEffect, useRef, useState } from 'react';

export default function Streaming(props) {
  const [actorProtoList, setActorProtoList] = useState(null);
  const [mvList, setMvList] = useState([]);
  const [fragments, setFragments] = useState([]);

  const message = useRef(null);

  useEffect(async () => {
    try {
      setActorProtoList(await getActors());
      setMvList(await getMaterializedViews());
      setFragments(await getFragments());
    } catch (e) {
      message.current.error(e.toString());
      console.error(e);
    }
  }, []);

  return (
    <>
      {actorProtoList
        && actorProtoList.length !== 0
        && actorProtoList[0].actors ?
        <StreamingView
          data={actorProtoList}
          mvList={mvList}
        />
        : <NoData />}
      <Message ref={message} vertical="top" horizontal="center" />
    </>
  )
}