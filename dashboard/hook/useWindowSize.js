import { useEffect, useState } from "react";

export default function useWindowSize() {
  const [size, setSize] = useState({ w: 1024, h: 768 });
  // setSize({
  //   w: window.innerWidth,
  //   h: window.innerHeight
  // })
  const setSizeOnEvent = () => {
    setSize({
      w: window.innerWidth,
      h: window.innerHeight
    })
  };
  useEffect(() => {
    window.addEventListener("resize", setSizeOnEvent);
    return () => window.removeEventListener("resize", setSizeOnEvent);
  }, []);
  return size;
}
