export function iter(n, step) {
  for (let i = 0; i < n; ++i) {
    step(i);
  }
}

export function newNumberArray(length){
  let rtn = [];
  iter(length, () => {
    rtn.push(0);
  })
  return rtn;
}

export function newMatrix(n) {
  let rtn = [];
  iter(n, () => {
    rtn.push([]);
  })
  return rtn;
}