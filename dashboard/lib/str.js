export function capitalize(sentence){
  let words = sentence.split(" ");
  let s = "";
  for(let word of words){
    s += word.charAt(0).toUpperCase() + word.slice(1, word.length);
  }
  return s;
}