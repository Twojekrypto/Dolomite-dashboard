const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');

const root=process.env.DOLO_ROUTE_ROOT||path.resolve(__dirname,'..');
const source=fs.readFileSync(path.join(root,'route-loader.js'),'utf8');
const routes={'':'',dolo:'',odolo:'odolo',vedolo:'vedolo',portfolio:'portfolio',assets:'assets',tvl:'tvl',earn:'earn',borrow:'borrow',liquidation:'borrow',supply:'supply',rewards:'rewards',revenue:'revenue',history:'history'};
function boot(href,framed=false){
 const url=new URL(href),redirects=[],requests=[],writes=[];
 const window={location:{origin:url.origin,pathname:url.pathname,search:url.search,hash:url.hash,replace:value=>redirects.push(value)}};
 window.top=framed?{}:window;
 const context={window,console,setTimeout:()=>0,fetch:async value=>{requests.push(value);return {ok:true,text:async()=>'<html><head></head><body><main>Legacy content</main></body></html>'};},document:{open(){},write:value=>writes.push(value),close(){}}};
 vm.runInNewContext(source,context);
 return {window,redirects,requests,writes};
}

test('legacy entry points redirect to the matching DoloView tab before fetching data',async()=>{
 const suffix='?address=0x28da3dde285d8f1f87b2d858f89961bb8b9af180&chain=arbitrum&from=2026-10-01&next=https%3A%2F%2Fexample.org#positions';
 for(const [route,target] of Object.entries(routes)){
  for(const ending of ['', '/', '/index.html']){
   const oldPath='/Dolomite-dashboard'+(route?'/'+route:'')+ending;
   const result=boot('https://twojekrypto.github.io'+oldPath+suffix);
   assert.deepEqual(result.redirects,['https://doloview.pages.dev/'+target+suffix],oldPath);
   await result.window.loadDoloRoute({target:'must-not-fetch.html',version:'test'});
   assert.deepEqual(result.requests,[],oldPath);
  }
 }
});

test('data files, other projects and other origins are never redirected',()=>{
 const urls=[
  'https://twojekrypto.github.io/Dolomite-dashboard/dolo_price.json',
  'https://twojekrypto.github.io/Dolomite-dashboard/dolo-confirmed-cex.js',
  'https://twojekrypto.github.io/Dolomite-dashboard/data/earn/latest.json',
  'https://twojekrypto.github.io/Dolomite-dashboard/history/history.js',
  'https://twojekrypto.github.io/Other-project/portfolio/',
  'https://twojekrypto.github.io/Dolomite-dashboard/dolo-preview.html',
  'https://twojekrypto.github.io/Dolomite-dashboard/unknown/',
  'https://doloview.pages.dev/portfolio',
  'http://127.0.0.1:8894/Dolomite-dashboard/portfolio/',
  'https://twojekrypto.github.io.example.org/Dolomite-dashboard/',
 ];
 for(const url of urls)assert.deepEqual(boot(url).redirects,[],url);
});

test('embedded legacy views retain their existing loading behavior',async()=>{
 const result=boot('https://twojekrypto.github.io/Dolomite-dashboard/portfolio/',true);
 assert.deepEqual(result.redirects,[]);
 await result.window.loadDoloRoute({target:'../portfolio-preview.html',version:'preserved',base:'../',route:'portfolio'});
 assert.deepEqual(result.requests,['../portfolio-preview.html?v=preserved']);
 assert.equal(result.window.__DOLO_ROUTE,'portfolio');
 assert.match(result.writes[0],/<base href="\.\.\/">/);
 assert.match(result.writes[0],/<main>Legacy content<\/main>/);
});

test('every public tab loads the fresh redirect-capable route loader first',()=>{
 for(const route of Object.keys(routes)){
  const file=route?route+'/index.html':'index.html';
  const html=fs.readFileSync(path.join(root,file),'utf8');
  const scripts=[...html.matchAll(/<script\b[^>]*\bsrc="([^"]+)"/g)];
  assert.ok(scripts.length,file);
  assert.match(scripts[0][1],/route-loader\.js\?v=doloview-redirect-20261116$/,file);
 }
});
