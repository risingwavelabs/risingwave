(globalThis.TURBOPACK||(globalThis.TURBOPACK=[])).push(["object"==typeof document?document.currentScript:void 0,8030,(e,t,r)=>{var n,o;o=n||(n={}),function(t){var r="object"==typeof globalThis?globalThis:e.g,n=i(o);function i(e,t){return function(r,n){Object.defineProperty(e,r,{configurable:!0,writable:!0,value:n}),t&&t(r,n)}}void 0!==r.Reflect&&(n=i(r.Reflect,n)),t(n,r),void 0===r.Reflect&&(r.Reflect=o)}(function(e,t){var r,n=Object.prototype.hasOwnProperty,o="function"==typeof Symbol,i=o&&void 0!==Symbol.toPrimitive?Symbol.toPrimitive:"@@toPrimitive",a=o&&void 0!==Symbol.iterator?Symbol.iterator:"@@iterator",s="function"==typeof Object.create,u=({__proto__:[]})instanceof Array,l=!s&&!u,c={create:s?function(){return A(Object.create(null))}:u?function(){return A({__proto__:null})}:function(){return A({})},has:l?function(e,t){return n.call(e,t)}:function(e,t){return t in e},get:l?function(e,t){return n.call(e,t)?e[t]:void 0}:function(e,t){return e[t]}},f=Object.getPrototypeOf(Function),d="function"==typeof Map&&"function"==typeof Map.prototype.entries?Map:function(){var e={},t=[],r=function(){function e(e,t,r){this._index=0,this._keys=e,this._values=t,this._selector=r}return e.prototype["@@iterator"]=function(){return this},e.prototype[a]=function(){return this},e.prototype.next=function(){var e=this._index;if(e>=0&&e<this._keys.length){var r=this._selector(this._keys[e],this._values[e]);return e+1>=this._keys.length?(this._index=-1,this._keys=t,this._values=t):this._index++,{value:r,done:!1}}return{value:void 0,done:!0}},e.prototype.throw=function(e){throw this._index>=0&&(this._index=-1,this._keys=t,this._values=t),e},e.prototype.return=function(e){return this._index>=0&&(this._index=-1,this._keys=t,this._values=t),{value:e,done:!0}},e}();function n(){this._keys=[],this._values=[],this._cacheKey=e,this._cacheIndex=-2}return Object.defineProperty(n.prototype,"size",{get:function(){return this._keys.length},enumerable:!0,configurable:!0}),n.prototype.has=function(e){return this._find(e,!1)>=0},n.prototype.get=function(e){var t=this._find(e,!1);return t>=0?this._values[t]:void 0},n.prototype.set=function(e,t){var r=this._find(e,!0);return this._values[r]=t,this},n.prototype.delete=function(t){var r=this._find(t,!1);if(r>=0){for(var n=this._keys.length,o=r+1;o<n;o++)this._keys[o-1]=this._keys[o],this._values[o-1]=this._values[o];return this._keys.length--,this._values.length--,P(t,this._cacheKey)&&(this._cacheKey=e,this._cacheIndex=-2),!0}return!1},n.prototype.clear=function(){this._keys.length=0,this._values.length=0,this._cacheKey=e,this._cacheIndex=-2},n.prototype.keys=function(){return new r(this._keys,this._values,o)},n.prototype.values=function(){return new r(this._keys,this._values,i)},n.prototype.entries=function(){return new r(this._keys,this._values,s)},n.prototype["@@iterator"]=function(){return this.entries()},n.prototype[a]=function(){return this.entries()},n.prototype._find=function(e,t){if(!P(this._cacheKey,e)){this._cacheIndex=-1;for(var r=0;r<this._keys.length;r++)if(P(this._keys[r],e)){this._cacheIndex=r;break}}return this._cacheIndex<0&&t&&(this._cacheIndex=this._keys.length,this._keys.push(e),this._values.push(void 0)),this._cacheIndex},n;function o(e,t){return e}function i(e,t){return t}function s(e,t){return[e,t]}}(),h="function"==typeof Set&&"function"==typeof Set.prototype.entries?Set:function(){function e(){this._map=new d}return Object.defineProperty(e.prototype,"size",{get:function(){return this._map.size},enumerable:!0,configurable:!0}),e.prototype.has=function(e){return this._map.has(e)},e.prototype.add=function(e){return this._map.set(e,e),this},e.prototype.delete=function(e){return this._map.delete(e)},e.prototype.clear=function(){this._map.clear()},e.prototype.keys=function(){return this._map.keys()},e.prototype.values=function(){return this._map.keys()},e.prototype.entries=function(){return this._map.entries()},e.prototype["@@iterator"]=function(){return this.keys()},e.prototype[a]=function(){return this.keys()},e}(),p="function"==typeof WeakMap?WeakMap:function(){var e=c.create(),t=o();function r(){this._key=o()}return r.prototype.has=function(e){var t=i(e,!1);return void 0!==t&&c.has(t,this._key)},r.prototype.get=function(e){var t=i(e,!1);return void 0!==t?c.get(t,this._key):void 0},r.prototype.set=function(e,t){return i(e,!0)[this._key]=t,this},r.prototype.delete=function(e){var t=i(e,!1);return void 0!==t&&delete t[this._key]},r.prototype.clear=function(){this._key=o()},r;function o(){var t;do t="@@WeakMap@@"+function(){var e=function(){if("function"==typeof Uint8Array){var e=new Uint8Array(16);return"u">typeof crypto?crypto.getRandomValues(e):"u">typeof msCrypto?msCrypto.getRandomValues(e):a(e,16),e}return a(Array(16),16)}();e[6]=79&e[6]|64,e[8]=191&e[8]|128;for(var t="",r=0;r<16;++r){var n=e[r];(4===r||6===r||8===r)&&(t+="-"),n<16&&(t+="0"),t+=n.toString(16).toLowerCase()}return t}();while(c.has(e,t))return e[t]=!0,t}function i(e,r){if(!n.call(e,t)){if(!r)return;Object.defineProperty(e,t,{value:c.create()})}return e[t]}function a(e,t){for(var r=0;r<t;++r)e[r]=255*Math.random()|0;return e}}(),y=o?Symbol.for("@reflect-metadata:registry"):void 0,v=(!k(y)&&O(t.Reflect)&&Object.isExtensible(t.Reflect)&&(r=t.Reflect[y]),k(r)&&(r=function(){k(y)||void 0===t.Reflect||y in t.Reflect||"function"!=typeof t.Reflect.defineMetadata||(r=(e=t.Reflect).defineMetadata,n=e.hasOwnMetadata,o=e.getOwnMetadata,i=e.getOwnMetadataKeys,a=e.deleteMetadata,s=new p,u={isProviderFor:function(e,t){var r=s.get(e);return!!(!k(r)&&r.has(t))||!!i(e,t).length&&(k(r)&&(r=new h,s.set(e,r)),r.add(t),!0)},OrdinaryDefineOwnMetadata:r,OrdinaryHasOwnMetadata:n,OrdinaryGetOwnMetadata:o,OrdinaryOwnMetadataKeys:i,OrdinaryDeleteMetadata:a});var e,r,n,o,i,a,s,u,l,c,f,v=new p,g={registerProvider:function(e){if(!Object.isExtensible(g))throw Error("Cannot add provider to a frozen registry.");switch(!0){case u===e:break;case k(l):l=e;break;case l===e:break;case k(c):c=e;break;case c===e:break;default:void 0===f&&(f=new h),f.add(e)}},getProvider:m,setProvider:function(e,t,r){if(!function(e){if(k(e))throw TypeError();return l===e||c===e||!k(f)&&f.has(e)}(r))throw Error("Metadata provider not registered.");var n=m(e,t);if(n!==r){if(!k(n))return!1;var o=v.get(e);k(o)&&(o=new d,v.set(e,o)),o.set(t,r)}return!0}};return g;function m(e,t){var r,n=v.get(e);return k(n)||(r=n.get(t)),k(r)&&(r=function(e,t){if(!k(l)){if(l.isProviderFor(e,t))return l;if(!k(c)){if(c.isProviderFor(e,t))return l;if(!k(f))for(var r=z(f);;){var n=R(r);if(!n)return;var o=n.value;if(o.isProviderFor(e,t))return C(r),o}}}if(!k(u)&&u.isProviderFor(e,t))return u}(e,t),k(r)||(k(n)&&(n=new d,v.set(e,n)),n.set(t,r))),r}}()),!k(y)&&O(t.Reflect)&&Object.isExtensible(t.Reflect)&&Object.defineProperty(t.Reflect,y,{enumerable:!1,configurable:!1,writable:!1,value:r}),r),g=function(e){var t=new p,r={isProviderFor:function(e,r){var n=t.get(e);return!k(n)&&n.has(r)},OrdinaryDefineOwnMetadata:function(e,t,r,o){n(r,o,!0).set(e,t)},OrdinaryHasOwnMetadata:function(e,t,r){var o=n(t,r,!1);return!k(o)&&!!o.has(e)},OrdinaryGetOwnMetadata:function(e,t,r){var o=n(t,r,!1);if(!k(o))return o.get(e)},OrdinaryOwnMetadataKeys:function(e,t){var r=[],o=n(e,t,!1);if(k(o))return r;for(var i=z(o.keys()),a=0;;){var s=R(i);if(!s)return r.length=a,r;var u=s.value;try{r[a]=u}catch(e){try{C(i)}finally{throw e}}a++}},OrdinaryDeleteMetadata:function(e,r,o){var i=n(r,o,!1);if(k(i)||!i.delete(e))return!1;if(0===i.size){var a=t.get(r);k(a)||(a.delete(o),0===a.size&&t.delete(a))}return!0}};return v.registerProvider(r),r;function n(n,o,i){var a=t.get(n),s=!1;if(k(a)){if(!i)return;a=new d,t.set(n,a),s=!0}var u=a.get(o);if(k(u)){if(!i)return;if(u=new d,a.set(o,u),!e.setProvider(n,o,r))throw a.delete(o),s&&t.delete(n),Error("Wrong provider for target.")}return u}}(v);function m(e,t,r){var n=L(t,r,!1);return!k(n)&&!!n.OrdinaryHasOwnMetadata(e,t,r)}function b(e,t,r){var n=L(t,r,!1);if(!k(n))return n.OrdinaryGetOwnMetadata(e,t,r)}function w(e,t,r,n){L(r,n,!0).OrdinaryDefineOwnMetadata(e,t,r,n)}function x(e,t){var r=L(e,t,!1);return r?r.OrdinaryOwnMetadataKeys(e,t):[]}function _(e){if(null===e)return 1;switch(typeof e){case"undefined":return 0;case"boolean":return 2;case"string":return 3;case"symbol":return 4;case"number":return 5;case"object":return null===e?1:6;default:return 6}}function k(e){return void 0===e}function j(e){return null===e}function O(e){return"object"==typeof e?null!==e:"function"==typeof e}e("decorate",function(e,t,r,n){if(k(r)){if(!M(e)||!$(t))throw TypeError();for(var o=e,i=t,a=o.length-1;a>=0;--a){var s=(0,o[a])(i);if(!k(s)&&!j(s)){if(!$(s))throw TypeError();i=s}}return i}if(!M(e)||!O(t)||!O(n)&&!k(n)&&!j(n))throw TypeError();return j(n)&&(n=void 0),function(e,t,r,n){for(var o=e.length-1;o>=0;--o){var i=(0,e[o])(t,r,n);if(!k(i)&&!j(i)){if(!O(i))throw TypeError();n=i}}return n}(e,t,r=S(r),n)}),e("metadata",function(e,t){return function(r,n){if(!O(r)||!k(n)&&!function(e){switch(_(e)){case 3:case 4:return!0;default:return!1}}(n))throw TypeError();w(e,t,r,n)}}),e("defineMetadata",function(e,t,r,n){if(!O(r))throw TypeError();return k(n)||(n=S(n)),w(e,t,r,n)}),e("hasMetadata",function(e,t,r){if(!O(t))throw TypeError();return k(r)||(r=S(r)),function e(t,r,n){if(m(t,r,n))return!0;var o=I(r);return!j(o)&&e(t,o,n)}(e,t,r)}),e("hasOwnMetadata",function(e,t,r){if(!O(t))throw TypeError();return k(r)||(r=S(r)),m(e,t,r)}),e("getMetadata",function(e,t,r){if(!O(t))throw TypeError();return k(r)||(r=S(r)),function e(t,r,n){if(m(t,r,n))return b(t,r,n);var o=I(r);if(!j(o))return e(t,o,n)}(e,t,r)}),e("getOwnMetadata",function(e,t,r){if(!O(t))throw TypeError();return k(r)||(r=S(r)),b(e,t,r)}),e("getMetadataKeys",function(e,t){if(!O(e))throw TypeError();return k(t)||(t=S(t)),function e(t,r){var n=x(t,r),o=I(t);if(null===o)return n;var i=e(o,r);if(i.length<=0)return n;if(n.length<=0)return i;for(var a=new h,s=[],u=0;u<n.length;u++){var l=n[u],c=a.has(l);c||(a.add(l),s.push(l))}for(var f=0;f<i.length;f++){var l=i[f],c=a.has(l);c||(a.add(l),s.push(l))}return s}(e,t)}),e("getOwnMetadataKeys",function(e,t){if(!O(e))throw TypeError();return k(t)||(t=S(t)),x(e,t)}),e("deleteMetadata",function(e,t,r){if(!O(t)||(k(r)||(r=S(r)),!O(t)))throw TypeError();k(r)||(r=S(r));var n=L(t,r,!1);return!k(n)&&n.OrdinaryDeleteMetadata(e,t,r)});function S(e){var t=function(e){switch(_(e)){case 0:case 1:case 2:case 3:case 4:case 5:return e}var t="string",r=T(e,i);if(void 0!==r){var n=r.call(e,t);if(O(n))throw TypeError();return n}return function(e,t){if("string"===t){var r=e.toString;if(E(r)){var n=r.call(e);if(!O(n))return n}var o=e.valueOf;if(E(o)){var n=o.call(e);if(!O(n))return n}}else{var o=e.valueOf;if(E(o)){var n=o.call(e);if(!O(n))return n}var i=e.toString;if(E(i)){var n=i.call(e);if(!O(n))return n}}throw TypeError()}(e,t)}(e);return"symbol"==typeof t?t:""+t}function M(e){return Array.isArray?Array.isArray(e):e instanceof Object?e instanceof Array:"[object Array]"===Object.prototype.toString.call(e)}function E(e){return"function"==typeof e}function $(e){return"function"==typeof e}function P(e,t){return e===t||e!=e&&t!=t}function T(e,t){var r=e[t];if(null!=r){if(!E(r))throw TypeError();return r}}function z(e){var t=T(e,a);if(!E(t))throw TypeError();var r=t.call(e);if(!O(r))throw TypeError();return r}function R(e){var t=e.next();return!t.done&&t}function C(e){var t=e.return;t&&t.call(e)}function I(e){var t=Object.getPrototypeOf(e);if("function"!=typeof e||e===f||t!==f)return t;var r=e.prototype,n=r&&Object.getPrototypeOf(r);if(null==n||n===Object.prototype)return t;var o=n.constructor;return"function"!=typeof o||o===e?t:o}function L(e,t,r){var n=v.getProvider(e,t);if(!k(n))return n;if(r){if(v.setProvider(e,t,g))return g;throw Error("Illegal state.")}}function A(e){return e.__=void 0,delete e.__,e}})},48189,(e,t,r)=>{t.exports=e.r(78068)},92177,(e,t,r)=>{"use strict";Object.defineProperty(r,"__esModule",{value:!0}),Object.defineProperty(r,"useRouter",{enumerable:!0,get:function(){return i}});let n=e.r(89959),o=e.r(46603);function i(){return(0,n.useContext)(o.RouterContext)}("function"==typeof r.default||"object"==typeof r.default&&null!==r.default)&&void 0===r.default.__esModule&&(Object.defineProperty(r.default,"__esModule",{value:!0}),Object.assign(r.default,r),t.exports=r.default)},31171,(e,t,r)=>{t.exports=e.r(92177)},3115,e=>{"use strict";let t;var r=e.i(78902);e.i(8030);var n=e.i(22026),o=e.i(27170),i=e.i(99785),i=i,a=e.i(89959),s=e.i(92980);let u="chakra-ui-light",l="chakra-ui-dark",c="chakra-ui-color-mode",f={ssr:!1,type:"localStorage",get(e){let t;if(!globalThis?.document)return e;try{t=localStorage.getItem(c)||e}catch(e){}return t||e},set(e){try{localStorage.setItem(c,e)}catch(e){}}},d=()=>{},h=(0,o.isBrowser)()?a.useLayoutEffect:a.useEffect;function p(e,t){return"cookie"===e.type&&e.ssr?e.get(t):t}let y=function(e){let{value:t,children:n,options:{useSystemColorMode:o,initialColorMode:c,disableTransitionOnChange:y}={},colorModeManager:v=f}=e,g=(0,i._)(),m="dark"===c?"dark":"light",[b,w]=(0,a.useState)(()=>p(v,m)),[x,_]=(0,a.useState)(()=>p(v)),{getSystemTheme:k,setClassName:j,setDataset:O,addListener:S}=(0,a.useMemo)(()=>(function(e={}){let{preventTransition:t=!0,nonce:r}=e,n={setDataset:e=>{let r=t?n.preventTransition():void 0;document.documentElement.dataset.theme=e,document.documentElement.style.colorScheme=e,r?.()},setClassName(e){document.body.classList.add(e?l:u),document.body.classList.remove(e?u:l)},query:()=>window.matchMedia("(prefers-color-scheme: dark)"),getSystemTheme:e=>n.query().matches??"dark"===e?"dark":"light",addListener(e){let t=n.query(),r=t=>{e(t.matches?"dark":"light")};return"function"==typeof t.addListener?t.addListener(r):t.addEventListener("change",r),()=>{"function"==typeof t.removeListener?t.removeListener(r):t.removeEventListener("change",r)}},preventTransition(){let e=document.createElement("style");return e.appendChild(document.createTextNode("*{-webkit-transition:none!important;-moz-transition:none!important;-o-transition:none!important;-ms-transition:none!important;transition:none!important}")),void 0!==r&&(e.nonce=r),document.head.appendChild(e),()=>{window.getComputedStyle(document.body),requestAnimationFrame(()=>{requestAnimationFrame(()=>{document.head.removeChild(e)})})}}};return n})({preventTransition:y,nonce:g?.nonce}),[y,g?.nonce]),M="system"!==c||b?b:x,E=(0,a.useCallback)(e=>{let t="system"===e?k():e;w(t),j("dark"===t),O(t),v.set(t)},[v,k,j,O]);h(()=>{"system"===c&&_(k())},[]),(0,a.useEffect)(()=>{let e=v.get();e?E(e):"system"===c?E("system"):E(m)},[v,m,c,E]);let $=(0,a.useCallback)(()=>{E("dark"===M?"light":"dark")},[M,E]);(0,a.useEffect)(()=>{if(o)return S(E)},[o,S,E]);let P=(0,a.useMemo)(()=>({colorMode:t??M,toggleColorMode:t?d:$,setColorMode:t?d:E,forced:void 0!==t}),[M,$,E,t]);return(0,r.jsx)(s.ColorModeContext.Provider,{value:P,children:n})};y.displayName="ColorModeProvider";var v=e.i(67059);let g=String.raw,m=g`
  :root,
  :host {
    --chakra-vh: 100vh;
  }

  @supports (height: -webkit-fill-available) {
    :root,
    :host {
      --chakra-vh: -webkit-fill-available;
    }
  }

  @supports (height: -moz-fill-available) {
    :root,
    :host {
      --chakra-vh: -moz-fill-available;
    }
  }

  @supports (height: 100dvh) {
    :root,
    :host {
      --chakra-vh: 100dvh;
    }
  }
`,b=()=>(0,r.jsx)(v.Global,{styles:m}),w=({scope:e=""})=>(0,r.jsx)(v.Global,{styles:g`
      html {
        line-height: 1.5;
        -webkit-text-size-adjust: 100%;
        font-family: system-ui, sans-serif;
        -webkit-font-smoothing: antialiased;
        text-rendering: optimizeLegibility;
        -moz-osx-font-smoothing: grayscale;
        touch-action: manipulation;
      }

      body {
        position: relative;
        min-height: 100%;
        margin: 0;
        font-feature-settings: "kern";
      }

      ${e} :where(*, *::before, *::after) {
        border-width: 0;
        border-style: solid;
        box-sizing: border-box;
        word-wrap: break-word;
      }

      main {
        display: block;
      }

      ${e} hr {
        border-top-width: 1px;
        box-sizing: content-box;
        height: 0;
        overflow: visible;
      }

      ${e} :where(pre, code, kbd,samp) {
        font-family: SFMono-Regular, Menlo, Monaco, Consolas, monospace;
        font-size: 1em;
      }

      ${e} a {
        background-color: transparent;
        color: inherit;
        text-decoration: inherit;
      }

      ${e} abbr[title] {
        border-bottom: none;
        text-decoration: underline;
        -webkit-text-decoration: underline dotted;
        text-decoration: underline dotted;
      }

      ${e} :where(b, strong) {
        font-weight: bold;
      }

      ${e} small {
        font-size: 80%;
      }

      ${e} :where(sub,sup) {
        font-size: 75%;
        line-height: 0;
        position: relative;
        vertical-align: baseline;
      }

      ${e} sub {
        bottom: -0.25em;
      }

      ${e} sup {
        top: -0.5em;
      }

      ${e} img {
        border-style: none;
      }

      ${e} :where(button, input, optgroup, select, textarea) {
        font-family: inherit;
        font-size: 100%;
        line-height: 1.15;
        margin: 0;
      }

      ${e} :where(button, input) {
        overflow: visible;
      }

      ${e} :where(button, select) {
        text-transform: none;
      }

      ${e} :where(
          button::-moz-focus-inner,
          [type="button"]::-moz-focus-inner,
          [type="reset"]::-moz-focus-inner,
          [type="submit"]::-moz-focus-inner
        ) {
        border-style: none;
        padding: 0;
      }

      ${e} fieldset {
        padding: 0.35em 0.75em 0.625em;
      }

      ${e} legend {
        box-sizing: border-box;
        color: inherit;
        display: table;
        max-width: 100%;
        padding: 0;
        white-space: normal;
      }

      ${e} progress {
        vertical-align: baseline;
      }

      ${e} textarea {
        overflow: auto;
      }

      ${e} :where([type="checkbox"], [type="radio"]) {
        box-sizing: border-box;
        padding: 0;
      }

      ${e} input[type="number"]::-webkit-inner-spin-button,
      ${e} input[type="number"]::-webkit-outer-spin-button {
        -webkit-appearance: none !important;
      }

      ${e} input[type="number"] {
        -moz-appearance: textfield;
      }

      ${e} input[type="search"] {
        -webkit-appearance: textfield;
        outline-offset: -2px;
      }

      ${e} input[type="search"]::-webkit-search-decoration {
        -webkit-appearance: none !important;
      }

      ${e} ::-webkit-file-upload-button {
        -webkit-appearance: button;
        font: inherit;
      }

      ${e} details {
        display: block;
      }

      ${e} summary {
        display: list-item;
      }

      template {
        display: none;
      }

      [hidden] {
        display: none !important;
      }

      ${e} :where(
          blockquote,
          dl,
          dd,
          h1,
          h2,
          h3,
          h4,
          h5,
          h6,
          hr,
          figure,
          p,
          pre
        ) {
        margin: 0;
      }

      ${e} button {
        background: transparent;
        padding: 0;
      }

      ${e} fieldset {
        margin: 0;
        padding: 0;
      }

      ${e} :where(ol, ul) {
        margin: 0;
        padding: 0;
      }

      ${e} textarea {
        resize: vertical;
      }

      ${e} :where(button, [role="button"]) {
        cursor: pointer;
      }

      ${e} button::-moz-focus-inner {
        border: 0 !important;
      }

      ${e} table {
        border-collapse: collapse;
      }

      ${e} :where(h1, h2, h3, h4, h5, h6) {
        font-size: inherit;
        font-weight: inherit;
      }

      ${e} :where(button, input, optgroup, select, textarea) {
        padding: 0;
        line-height: inherit;
        color: inherit;
      }

      ${e} :where(img, svg, video, canvas, audio, iframe, embed, object) {
        display: block;
      }

      ${e} :where(img, video) {
        max-width: 100%;
        height: auto;
      }

      [data-js-focus-visible]
        :focus:not([data-focus-visible-added]):not(
          [data-focus-visible-disabled]
        ) {
        outline: none;
        box-shadow: none;
      }

      ${e} select::-ms-expand {
        display: none;
      }

      ${m}
    `});var x=e.i(63707),_=e.i(66607),k=e.i(45237),j=e.i(17656),O=e.i(59873);function S(e,t,r={}){let{stop:n,getKey:o}=r;return function e(r,i=[]){if((0,_.isObject)(r)||Array.isArray(r)){let a={};for(let[s,u]of Object.entries(r)){let l=o?.(s)??s,c=[...i,l];if(n?.(r,c))return t(r,i);a[l]=e(u,c)}return a}return t(r,i)}(e)}var M=e.i(76851);let E=["colors","borders","borderWidths","borderStyles","fonts","fontSizes","fontWeights","gradients","letterSpacings","lineHeights","radii","space","shadows","sizes","zIndices","transition","blur","breakpoints"];function $(e,t){return(0,O.cssVar)(String(e).replace(/\./g,"-"),void 0,t)}var P=e.i(58596),T=e.i(51201),z=e.i(41548),R=e.i(28348),C=i;function I(e){let{cssVarsRoot:t,theme:n,children:o}=e,i=(0,a.useMemo)(()=>(function(e){let t=function(e){let{__cssMap:t,__cssVars:r,__breakpoints:n,...o}=e;return o}(e),{cssMap:r,cssVars:n}=function(e){var t;let r,n,o,i,a=(r=function(e){let t={};for(let r of E)r in e&&(t[r]=e[r]);return t}(t=e),n=t.semanticTokens,o=e=>M.pseudoPropNames.includes(e)||"default"===e,i={},S(r,(e,t)=>{null!=e&&(i[t.join(".")]={isSemantic:!1,value:e})}),S(n,(e,t)=>{null!=e&&(i[t.join(".")]={isSemantic:!0,value:e})},{stop:e=>Object.keys(e).every(o)}),i),s=e.config?.cssVarPrefix,u={},l={};for(let[e,t]of Object.entries(a)){let{isSemantic:r,value:n}=t,{variable:o,reference:i}=$(e,s);if(!r){if(e.startsWith("space")){let[t,...r]=e.split("."),a=`${t}.-${r.join(".")}`,s=j.calc.negate(n),u=j.calc.negate(i);l[a]={value:s,var:o,varRef:u}}u[o]=n,l[e]={value:n,var:o,varRef:i};continue}let c=(0,_.isObject)(n)?n:{default:n};u=(0,k.mergeWith)(u,Object.entries(c).reduce((t,[r,n])=>{if(!n)return t;let i=function(e,t){let r=[String(e).split(".")[0],t].join(".");if(!a[r])return t;let{reference:n}=$(r,s);return n}(e,`${n}`);return"default"===r?t[o]=i:t[M.pseudoSelectors?.[r]??r]={[o]:i},t},{})),l[e]={value:i,var:o,varRef:i}}return{cssVars:u,cssMap:l}}(t);return Object.assign(t,{__cssVars:{"--chakra-ring-inset":"var(--chakra-empty,/*!*/ /*!*/)","--chakra-ring-offset-width":"0px","--chakra-ring-offset-color":"#fff","--chakra-ring-color":"rgba(66, 153, 225, 0.6)","--chakra-ring-offset-shadow":"0 0 #0000","--chakra-ring-shadow":"0 0 #0000","--chakra-space-x-reverse":"0","--chakra-space-y-reverse":"0",...n},__cssMap:r,__breakpoints:(0,x.analyzeBreakpoints)(t.breakpoints)}),t})(n),[n]);return(0,r.jsxs)(C.a,{theme:i,children:[(0,r.jsx)(L,{root:t}),o]})}function L({root:e=":host, :root"}){let t=[e,"[data-theme]"].join(",");return(0,r.jsx)(v.Global,{styles:e=>({[t]:e.__cssVars})})}let[A,F]=(0,T.createContext)({name:"StylesContext",errorMessage:"useStyles: `styles` is undefined. Seems you forgot to wrap the components in `<StylesProvider />` "});function N(){let{colorMode:e}=(0,s.useColorMode)();return(0,r.jsx)(v.Global,{styles:t=>{let r=(0,z.memoizedGet)(t,"styles.global"),n=(0,R.runIfFn)(r,{theme:t,colorMode:e});if(n)return(0,P.css)(n)(t)}})}var W=e.i(60819),q=e.i(95273);let D=(0,a.createContext)({getDocument:()=>document,getWindow:()=>window});function B(e){let{children:t,environment:n,disabled:o}=e,i=(0,a.useRef)(null),s=(0,a.useMemo)(()=>n||{getDocument:()=>i.current?.ownerDocument??document,getWindow:()=>i.current?.ownerDocument.defaultView??window},[n]),u=!o||!n;return(0,r.jsxs)(D.Provider,{value:s,children:[t,u&&(0,r.jsx)("span",{id:"__chakra_env",hidden:!0,ref:i})]})}D.displayName="EnvironmentContext",B.displayName="EnvironmentProvider";let U=e=>{let{children:t,colorModeManager:n,portalZIndex:o,resetScope:i,resetCSS:a=!0,theme:s={},environment:u,cssVarsRoot:l,disableEnvironment:c,disableGlobalStyle:f}=e,d=(0,r.jsx)(B,{environment:u,disabled:c,children:t});return(0,r.jsx)(I,{theme:s,cssVarsRoot:l,children:(0,r.jsxs)(y,{colorModeManager:n,options:s.config,children:[a?(0,r.jsx)(w,{scope:i}):(0,r.jsx)(b,{}),!f&&(0,r.jsx)(N,{}),o?(0,r.jsx)(W.PortalManager,{zIndex:o,children:d}):d]})})};var H=e.i(24023);let K=(t=n.theme,function({children:e,theme:n=t,toastOptions:o,...i}){return(0,r.jsxs)(U,{theme:n,...i,children:[(0,r.jsx)(H.ToastOptionProvider,{value:o?.defaultOptions,children:e}),(0,r.jsx)(H.ToastProvider,{...o})]})});e.i(42113);var G=e.i(17873),G=G,V=e.i(48189),X=e.i(39564),Q=e.i(15004),Y=e.i(46835);(0,X.o)("queue-reset",()=>({mutex:0}));var J=e.i(31171);let Z=(0,X.o)("next-pages-router-update",()=>({isNuqsUpdate:!1}));function ee(){Z.isNuqsUpdate||((0,Q.t)(19),Y.t.abortAll(),Y.r.abort().forEach(e=>Y.t.queuedQuerySync.emit(e)))}let et=(0,X.n)(function(){let e=(0,J.useRouter)();return(0,a.useEffect)(()=>(e?.events.on("routeChangeStart",ee),e?.events.on("beforeHistoryChange",ee),()=>{e?.events.off("routeChangeStart",ee),e?.events.off("beforeHistoryChange",ee)}),[]),{searchParams:(0,a.useMemo)(()=>{let t=new URLSearchParams;if(null===e)return t;for(let[r,n]of Object.entries(e.query))if("string"==typeof n)t.set(r,n);else if(Array.isArray(n))for(let e of n)t.append(r,e);return t},[JSON.stringify(e?.query)]),updateUrl:(0,a.useCallback)((e,t)=>{let r=window.next?.router,n=function(e,t){let r,n=new Set,o=/\[([^\]]+)\]/g;for(;null!==(r=o.exec(e));){let e=r[1];e&&n.add(e)}let i=Object.fromEntries(Object.entries(t).filter(([e])=>n.has(e))),a=/\[\.{3}([^\]]+)\]$/.exec(e);if(a&&a[1]){let e=a[1];i[e]=t[e]??[]}let s=/\[\[\.{3}([^\]]+)\]\]$/.exec(e);if(s&&s[1]){let e=s[1];i[e]=t[e]??[]}return i}(r.pathname,r.query),o=r.asPath.replace(/#.*$/,"").replace(/\?.*$/,"")+(0,X.c)(e)+location.hash;(0,Q.t)(20,"next/pages",o);let i="push"===t.history?r.push:r.replace;Z.isNuqsUpdate=!0;try{i.call(r,{pathname:r.pathname,query:{...function(e){let t={};for(let r of e.keys()){let n=e.getAll(r);1===n.length?t[r]=n[0]:n.length>1&&(t[r]=n)}return t}(e),...n}},o,{scroll:t.scroll,shallow:t.shallow}).finally(()=>{Z.isNuqsUpdate=!1})}catch(e){throw Z.isNuqsUpdate=!1,e}},[]),autoResetQueueOnUpdate:!1}});var er=e.i(35685),en=e.i(18698),eo=e.i(38890),ei=e.i(36943),ea=e.i(16333);let es=(0,ea.forwardRef)(function(e,t){let{htmlWidth:n,htmlHeight:o,alt:i,...a}=e;return(0,r.jsx)("img",{width:n,height:o,ref:t,alt:i,...a})});es.displayName="NativeImage";var eu=e.i(94299);let el=(0,ea.forwardRef)(function(e,t){let n,{fallbackSrc:o,fallback:i,src:s,srcSet:u,align:l,fit:c,loading:f,ignoreFallback:d,crossOrigin:h,fallbackStrategy:p="beforeLoadOrError",referrerPolicy:y,...v}=e,g=void 0!==o||void 0!==i,m=null!=f||d||!g,b=(n=function(e){let{loading:t,src:r,srcSet:n,onLoad:o,onError:i,crossOrigin:s,sizes:u,ignoreFallback:l}=e,[c,f]=(0,a.useState)("pending");(0,a.useEffect)(()=>{f(r?"loading":"pending")},[r]);let d=(0,a.useRef)(null),h=(0,a.useCallback)(()=>{if(!r)return;p();let e=new Image;e.src=r,s&&(e.crossOrigin=s),n&&(e.srcset=n),u&&(e.sizes=u),t&&(e.loading=t),e.onload=e=>{p(),f("loaded"),o?.(e)},e.onerror=e=>{p(),f("failed"),i?.(e)},d.current=e},[r,s,n,u,o,i,t]),p=()=>{d.current&&(d.current.onload=null,d.current.onerror=null,d.current=null)};return(0,q.useSafeLayoutEffect)(()=>{if(!l)return"loading"===c&&h(),()=>{p()}},[c,h,l]),l?"loaded":c}({...e,crossOrigin:h,ignoreFallback:m}),"loaded"!==n&&"beforeLoadOrError"===p||"failed"===n&&"onError"===p),w={ref:t,objectFit:c,objectPosition:l,...m?v:(0,ei.omit)(v,["onError","onLoad"])};return b?i||(0,r.jsx)(eu.chakra.img,{as:es,className:"chakra-image__placeholder",src:o,...w}):(0,r.jsx)(eu.chakra.img,{as:es,src:s,srcSet:u,crossOrigin:h,loading:f,referrerPolicy:y,className:"chakra-image",...w})});el.displayName="Image";var ec=e.i(68931),ef=e.i(29239),ed=e.i(35586);let eh=(0,ea.forwardRef)(function(e,t){let n=(0,ed.useStyleConfig)("Link",e),{className:o,isExternal:i,...a}=(0,ec.omitThemingProps)(e);return(0,r.jsx)(eu.chakra.a,{target:i?"_blank":void 0,rel:i?"noopener":void 0,ref:t,className:(0,ef.cx)("chakra-link",o),...a,__css:n})});eh.displayName="Link";var ep=e.i(19319),ey=e.i(47095),ev=e.i(857),eg=e.i(69062);let em="216px",eb=[{items:[{href:"/cluster/",title:"Cluster overview",icon:ev.IconServer}]},{label:"Catalog",items:[{href:"/sources/",title:"Sources",icon:ev.IconArrowDownToLine},{href:"/tables/",title:"Tables",icon:ev.IconTable},{href:"/materialized_views/",title:"Materialized views",icon:ev.IconLayers},{href:"/indexes/",title:"Indexes",icon:ev.IconListTree},{href:"/internal_tables/",title:"Internal tables",icon:ev.IconDatabase},{href:"/sinks/",title:"Sinks",icon:ev.IconArrowUpFromLine},{href:"/views/",title:"Views",icon:ev.IconEye},{href:"/subscriptions/",title:"Subscriptions",icon:ev.IconRss},{href:"/functions/",title:"Functions",icon:ev.IconSquareFunction}]},{label:"Streaming",items:[{href:"/relation_graph/",title:"Relation graph",icon:ev.IconWorkflow},{href:"/fragment_graph/",title:"Fragment graph",icon:ev.IconGitBranch}]},{label:"Batch",items:[{href:"/batch_tasks/",title:"Batch tasks",icon:ev.IconListChecks}]},{label:"Explain",items:[{href:"/explain_distsql/",title:"Distributed plan",icon:ev.IconNetwork}]},{label:"Debug",items:[{href:"/await_tree/",title:"Await tree dump",icon:ev.IconHourglass},{href:"/heap_profiling/",title:"Heap profiling",icon:ev.IconMemoryStick},{href:"/api/monitor/diagnose",title:"Diagnose",icon:ev.IconActivity,external:!0},{href:"/trace/search",title:"Traces",icon:ev.IconRoute,external:!0}]},{label:"Settings",items:[{href:"/settings/",title:"Settings",icon:ev.IconSettings}]}];function ew({href:e,title:t,icon:n,external:o}){let i=(0,V.useRouter)(),[s,u]=(0,a.useState)(!1);return(0,a.useEffect)(()=>{o||u(`${i.asPath.replace(/\/+$/,"")}/`.startsWith(e.toString()))},[e,i.asPath,o]),(0,r.jsxs)(eh,{as:ey.default,href:e,prefetch:!1,target:o?"_blank":void 0,rel:o?"noreferrer":void 0,display:"flex",alignItems:"center",gap:2,px:2.5,py:"7px",borderRadius:eg.radii.md,fontSize:"14px",lineHeight:1.5,fontWeight:s?500:400,color:s?eg.colors.foreground:eg.colors.mutedForeground,bg:s?eg.fills.active:"transparent",textDecoration:"none",transition:`background-color ${eg.motion.durationMs.micro}ms ${eg.motion.easeOut}, color ${eg.motion.durationMs.micro}ms ${eg.motion.easeOut}`,_hover:{bg:s?eg.fills.active:eg.fills.hover,color:eg.colors.foreground,textDecoration:"none"},children:[(0,r.jsx)(er.Box,{flexShrink:0,children:(0,r.jsx)(n,{size:15})}),(0,r.jsx)(er.Box,{as:"span",flex:1,noOfLines:1,children:t}),o&&(0,r.jsx)(er.Box,{flexShrink:0,opacity:.5,children:(0,r.jsx)(ev.IconArrowUpRight,{size:12})})]})}function ex({label:e,items:t}){return(0,r.jsxs)(er.Box,{width:"full",mt:4*!!e,children:[e&&(0,r.jsx)(ep.Text,{px:2.5,mb:1,fontSize:"12px",fontWeight:500,lineHeight:1.4,color:eg.colors.mutedForeground,children:e}),(0,r.jsx)(en.Flex,{direction:"column",gap:"2px",children:t.map(e=>(0,r.jsx)(ew,{...e},e.href.toString()))})]})}let e_=function({children:e}){return(0,r.jsxs)(en.Flex,{bg:eg.colors.background,children:[(0,r.jsxs)(er.Box,{height:"100vh",overflowY:"auto",width:em,minWidth:em,bg:"rgba(245,244,240,0.5)",borderRight:"1px solid",borderColor:"rgba(227,224,216,0.5)",py:2,px:2,fontFamily:eg.fonts.body,color:eg.colors.foreground,children:[(0,r.jsxs)(eo.HStack,{height:"52px",spacing:2,px:2.5,mb:1,children:[(0,r.jsx)(ey.default,{href:"/",children:(0,r.jsx)(el,{boxSize:"20px",src:"/risingwave.svg",alt:"RisingWave Logo"})}),(0,r.jsx)(ep.Text,{fontSize:"14px",fontWeight:600,letterSpacing:"-0.01em",children:"RisingWave"}),(0,r.jsx)(er.Box,{as:"span",px:2,py:"1px",borderRadius:"full",fontSize:"12px",fontWeight:500,lineHeight:1.45,color:eg.colors.mutedForeground,bg:eg.fills.active,children:"Dashboard"})]}),eb.map((e,t)=>(0,r.jsx)(ex,{...e},e.label??t))]}),(0,r.jsx)(er.Box,{flex:1,minWidth:0,overflowY:"auto",maxHeight:"100vh",children:e})]})};var ek=e.i(21018);e.s(["default",0,function({Component:e,pageProps:t}){let n=(0,V.useRouter)(),[o,i]=(0,a.useState)(!1);return(0,a.useEffect)(()=>{n.events.on("routeChangeStart",(e,{shallow:t})=>{t||i(!0)}),n.events.on("routeChangeComplete",()=>i(!1)),n.events.on("routeChangeError",()=>i(!1))},[n.events]),(0,a.useEffect)(()=>{G.default.config({paths:{vs:"/monaco"}})}),(0,r.jsx)(et,{children:(0,r.jsx)(K,{children:(0,r.jsx)(e_,{children:o?(0,r.jsx)(ek.default,{}):(0,r.jsx)(e,{...t})})})})}],3115)},68146,(e,t,r)=>{let n="/_app";(window.__NEXT_P=window.__NEXT_P||[]).push([n,()=>e.r(3115)]),t.hot&&t.hot.dispose(function(){window.__NEXT_P.push([n])})}]);