"""Exact original-contract formula quotes for two known retired ARB oracle paths.

rpc(method, params, anchor) must be the collector's existing paced, immutable-cache
read callback. This module does no networking, retries, sleeps or price estimation.
Returned prices are *reconstructed formula marks*, never active Margin marks or
promises that the whole position can currently be redeemed.
"""
import hashlib,json,re

class RecoveryRejected(ValueError):
    """The narrow historical reconstruction could not be proved."""

MARGIN='0x6bd780e7fdf01d77e4d475c821f1e7ae05409072'
AGG='0xbfca44ab734e57dc823ca609a0714eec9ed06ca0'
AUTO='0x0bd39c5a7b2db42993c9978335b497771ccb0efd'
PAUSE='0x53e18f7483356cac74b1365ebf3f2f71f499a0dd'
LEGACY='0x11e4e70b4ac087624f55d1dccab306440250a3a0'
PT_ORACLE='0x162abce9bbf515cf86af4b5fbfa31979aabc35ca'
PT_REGISTRY='0x1572ba6f148240ee2205d38de91c23d644d5b733'
PT_RATE_ORACLE='0x7e16e4253ce4a1c96422a9567b23b4b5ebc207f1'
PT_MARKET='0x551c423c441db0b691b5630f04d2080caee25963'
GLP='0x5402b5f40310bded796c7d0f3ff6683f5c0cffdf'
VAULT='0x5326e71ff593ecc2cf7acae5fe57582d6e74cff1'
PLV_REGISTRY='0x6fceb6c2930d274b703b02a94eebaac7ecc6d1bb'
ROUTER='0xeae85745232983cf117692a1ce2ecf3d19ada683'
LEGACY_UNWRAPPER='0x025332f3524b1864bf6e6d93d2bbd4deee216c24'
ACTIVE_UNWRAPPER='0x74d3cb3955e7517aec82d391c5767be50dba575f'
TOKENS={9:'0x5c80ac681b6b0e7ef6e0751211012601e6cfb043',41:'0x0c4d46076af67f8ba1cc3c01f7e873bd91ea41ab'}
CODE_HASHES={
 AUTO:'4499778efd2d6c392ba59a52b291d1c2c0a1b589ae55c602929e18627e771e6d',
 AGG:'382f11c1869dc37bc54a80706ff341505176d0c473fa1fec384e63aec19b54b2',
 LEGACY:'0d65d45e59c8baa6278e364feb96463c70bc92fec76a9f977823a11038042f40',
 PT_ORACLE:'79789295f4b6c0a268d197f2ff48ff89064ce85402e13ef8c88e1377affb68ac',
 PAUSE:'018d5e929b4dec69a74ee053553f0a75bbb37d69f7f7a320824d4f03befeaef1',
}
CODE_RESULT_HASHES={'0x11e4e70b4ac087624f55d1dccab306440250a3a0': '383ea498fce9cd010d4a232db105f7c4d22965164c522dd7fc7113486edba498', '0x162abce9bbf515cf86af4b5fbfa31979aabc35ca': '4f556630f4813ccbce491e96bb5ba9640919563b3d5d944c0c1d81f133612301', '0x53e18f7483356cac74b1365ebf3f2f71f499a0dd': '3fc83325cf34f67fb66314ead889278f619c40c0c99591caaca815f58c74cc78', '0x0bd39c5a7b2db42993c9978335b497771ccb0efd': '000e1c49a214dfe21d1bba13042903ac31a7cffc8bbb23dd7fbfec605c9099aa', '0xbfca44ab734e57dc823ca609a0714eec9ed06ca0': 'a7fb3180b20652f1eaa8c3b23a3b1533d631b118187336e1b53485e95894962a'}
# Literal selectors from the official contract ABIs; no external ABI dependency.
SEL={'getMarketPrice': '8928378e', 'getMarketPriceOracle': '13368364', 'getMarketTokenAddress': '062bd3e9', 'getOraclesByToken': 'c5f5a062', 'getPrice': '41976e09', 'tokenToPaused': '64d81e5f', 'totalAssets': '01e1d114', 'totalSupply': '18160ddd', 'getFeeBp': 'ace1487f', 'REGISTRY': '06433b1b', 'plvGlpToken': '512084cd', 'plvGlpRouter': '8b7f9d6e', 'getPtToAssetRate': 'abca0eab', 'deductionCoefficient': '3fc57d2a', 'ptOracle': '05d7b9a2', 'ptMarket': '54304fd5', 'PLUTUS_VAULT_REGISTRY': '2115ba3e', 'DFS_GLP_MARKET_ID': '48218835'}

def plutus_formula(glp_price,assets,shares,exit_fee_bp):
    if any(type(n) is not int or n<0 for n in [glp_price,assets,shares,exit_fee_bp]) or not shares or exit_fee_bp>10000:
        raise RecoveryRejected('Invalid exact formula input')
    gross=glp_price*assets//shares
    return gross-gross*exit_fee_bp//10000

def _arg(value):
    if isinstance(value,str):
        if not re.fullmatch('0x[0-9a-f]{40}',value):raise RecoveryRejected('Invalid address')
        return value[2:].zfill(64)
    if type(value) is not int or not 0<=value<2**256:raise RecoveryRejected('Invalid uint256')
    return format(value,'064x')

def _words(raw,count=None):
    if not isinstance(raw,str) or not re.fullmatch('0x(?:[0-9a-fA-F]{64})+',raw):raise RecoveryRejected('Malformed ABI words')
    out=[int(raw[i:i+64],16) for i in range(2,len(raw),64)]
    if count is not None and len(out)!=count:raise RecoveryRejected('Unexpected ABI size')
    return out

def recover_price(rpc,*,chain_id,margin,market_id,token,decimals,header):
    """Return {'price': int, 'proof': dict}; reject every unproved alternative.

    Existing current-index values must remain untouched in the caller. Keep the
    returned proof on each reconstructed quote and propagate its provenance to
    any row valued with that quote.
    """
    if chain_id!=42161 or margin.lower()!=MARGIN or market_id not in TOKENS or token.lower()!=TOKENS[market_id] or decimals!=18:
        raise RecoveryRejected('Unsupported recovery identity')
    token=token.lower();number=header.get('number');anchor=header.get('hash')
    if type(number) is not int or number<0 or not isinstance(anchor,str) or not re.fullmatch('0x[0-9a-fA-F]{64}',anchor):raise RecoveryRejected('Missing pinned header')
    tag=hex(number);calls=[];runtime_hashes={}
    def read(method,params):
        try:result=rpc(method,params,anchor)
        except Exception as error:raise RecoveryRejected('Reconstruction read failed') from error
        calls.append({'method':method,'params':params,'resultHash':hashlib.sha256(json.dumps(result,sort_keys=True,separators=(',',':')).encode()).hexdigest()})
        return result
    def call(to,name,args=(),count=1):return _words(read('eth_call',[{'to':to,'data':'0x'+SEL[name]+''.join(_arg(a) for a in args)},tag]),count)
    def one(to,name,args=()):return call(to,name,args)[0]
    def address(to,name,args=()):
        n=one(to,name,args)
        if n>=2**160:raise RecoveryRejected('Malformed ABI address')
        return '0x'+format(n,'040x')
    def same(actual,expected):
        if actual!=expected:raise RecoveryRejected('Unexpected reconstruction configuration')
    def code(to):
        raw=read('eth_getCode',[to,tag])
        if not isinstance(raw,str) or not re.fullmatch('0x(?:[0-9a-fA-F]{2})+',raw):raise RecoveryRejected('Missing runtime code')
        observed=hashlib.sha256(bytes.fromhex(raw[2:])).hexdigest();same(observed,CODE_HASHES[to]);same(hashlib.sha256(json.dumps(raw,separators=(',',':')).encode()).hexdigest(),CODE_RESULT_HASHES[to]);runtime_hashes[to]=observed
    def oracle_config(to,oracle,pair):
        vals=call(AGG,'getOraclesByToken',[to],5);same(vals,[32,1,int(oracle,16),int(pair,16),100])
    expected='ChainlinkAutomationPriceOracle: Price is expired' if market_id==9 else 'Storage: Price cannot be zero <41>'
    direct_params=[{'to':MARGIN,'data':'0x'+SEL['getMarketPrice']+_arg(market_id)},tag]
    try:rpc('eth_call',direct_params,anchor)
    except ValueError as error:
        reason_bytes=expected.encode()
        expected_data='0x08c379a0'+format(32,'064x')+format(len(reason_bytes),'064x')+reason_bytes.hex().ljust(((len(reason_bytes)+31)//32)*64,'0')
        direct_hash=getattr(error,'response_hash',None)
        if (type(getattr(error,'code',None)) is not int or error.code!=3 or
            getattr(error,'message',None)!='execution reverted: '+expected or
            getattr(error,'data',None)!=expected_data or
            not isinstance(direct_hash,str) or not re.fullmatch('[0-9a-f]{64}',direct_hash)):
            raise RecoveryRejected('Unexpected or unstructured direct quote failure') from error
    except Exception as error:raise RecoveryRejected('Direct quote transport failure') from error
    else:raise RecoveryRejected('Direct quote is available')
    expected_error={'id':1,'jsonrpc':'2.0','error':{'code':3,'message':'execution reverted: '+expected,'data':expected_data}}
    same(direct_hash,hashlib.sha256(json.dumps(expected_error,sort_keys=True,separators=(',',':')).encode()).hexdigest())
    configured=AGG if market_id==9 else PAUSE
    same(address(MARGIN,'getMarketPriceOracle',[market_id]),configured)
    same(address(MARGIN,'getMarketTokenAddress',[market_id]),token)
    code(AGG)
    glp_price=one(MARGIN,'getMarketPrice',[6])
    if glp_price<=0:raise RecoveryRejected('Missing active GLP mark')
    if market_id==9:
        code(AUTO);code(LEGACY);oracle_config(token,AUTO,'0x'+'0'*40)
        same(address(AUTO,'PLUTUS_VAULT_REGISTRY'),PLV_REGISTRY)
        same(address(PLV_REGISTRY,'plvGlpToken'),VAULT);same(address(PLV_REGISTRY,'plvGlpRouter'),ROUTER)
        assets=one(VAULT,'totalAssets');shares=one(VAULT,'totalSupply')
        legacy_fee=call(ROUTER,'getFeeBp',[LEGACY_UNWRAPPER],2);active_fee=call(ROUTER,'getFeeBp',[ACTIVE_UNWRAPPER],2);fee=legacy_fee[0]
        same(fee,active_fee[0])
        price=plutus_formula(glp_price,assets,shares,fee)
        same(one(LEGACY,'getPrice',[token]),price)
        inputs={'glpQuote':str(glp_price),'assets':str(assets),'shares':str(shares),'exitFeeBp':str(fee),'legacyFeeReturn':[str(n) for n in legacy_fee],'activeFeeReturn':[str(n) for n in active_fee]}
        fallback=LEGACY
    else:
        code(PAUSE);code(PT_ORACLE);same(one(PAUSE,'tokenToPaused',[token]),1);oracle_config(token,PT_ORACLE,GLP)
        same(address(PT_ORACLE,'REGISTRY'),PT_REGISTRY)
        same(address(PT_REGISTRY,'ptOracle'),PT_RATE_ORACLE);same(address(PT_REGISTRY,'ptMarket'),PT_MARKET)
        rate=one(PT_RATE_ORACLE,'getPtToAssetRate',[PT_MARKET,900]);same(rate,10**18)
        same(one(PT_ORACLE,'deductionCoefficient'),0);same(one(PT_ORACLE,'getPrice',[token]),rate)
        price=glp_price*rate//10**18;same(one(AGG,'getPrice',[token]),price)
        inputs={'glpQuote':str(glp_price),'ptToAssetRate':str(rate),'deductionCoefficient':'0'};fallback=AGG
    if price<=0:raise RecoveryRejected('Reconstructed price is not positive')
    result={'price':price,'proof':{'method':'official_contract_formula_reconstruction_v1','chainId':42161,'marketId':str(market_id),'token':token,'blockNumber':number,'blockHash':anchor,'directOracle':configured,'directFailure':expected,'directErrorCode':3,'directResponseHash':direct_hash,'directErrorData':expected_data,'directCall':{'method':'eth_call','params':direct_params},'formulaOracle':fallback,'price':str(price),'decimals':18,'runtimeCodeHashes':runtime_hashes,'priceUnit':'USD*10**18; Wei*price gives USD*10**36','inputs':inputs,'calls':calls,'provesExecutableExit':False}}
    validate_reconstruction(result['proof'])
    return result

def validate_reconstruction(proof):
    """Recompute a compact public quote proof; the caller binds its endpoint.

    Raw source availability is attested by the collector's separate immutable
    cache manifest. This validates exact formula/configuration consistency and
    ties each required ABI return to its captured per-call result hash.
    """
    def require(condition):
        if not condition:raise RecoveryRejected('Invalid reconstruction proof')
    def uint(value):
        require(isinstance(value,str) and re.fullmatch('0|[1-9][0-9]*',value) is not None)
        n=int(value);require(n<2**256);return n
    try:
        p=proof;m=int(p['marketId']);require(str(m)==p['marketId'] and m in TOKENS)
        require(p['method']=='official_contract_formula_reconstruction_v1' and type(p['chainId']) is int and p['chainId']==42161)
        require(p['token']==TOKENS[m] and type(p['decimals']) is int and p['decimals']==18)
        require(type(p['blockNumber']) is int and p['blockNumber']>=0 and re.fullmatch('0x[0-9a-fA-F]{64}',p['blockHash']) is not None)
        require(p['provesExecutableExit'] is False and p['priceUnit']=='USD*10**18; Wei*price gives USD*10**36')
        expected='ChainlinkAutomationPriceOracle: Price is expired' if m==9 else 'Storage: Price cannot be zero <41>'
        require(p['directFailure']==expected and type(p['directErrorCode']) is int and p['directErrorCode']==3)
        require(re.fullmatch('[0-9a-f]{64}',p['directResponseHash']) is not None)
        reason=expected.encode();error_data='0x08c379a0'+format(32,'064x')+format(len(reason),'064x')+reason.hex().ljust(((len(reason)+31)//32)*64,'0')
        require(p['directErrorData']==error_data)
        envelope={'id':1,'jsonrpc':'2.0','error':{'code':3,'message':'execution reverted: '+expected,'data':error_data}}
        require(p['directResponseHash']==hashlib.sha256(json.dumps(envelope,sort_keys=True,separators=(',',':')).encode()).hexdigest())
        tag=hex(p['blockNumber']);token=TOKENS[m]
        require(p['directCall']=={'method':'eth_call','params':[{'to':MARGIN,'data':'0x'+SEL['getMarketPrice']+_arg(m)},tag]})
        require(p['directOracle']==(AGG if m==9 else PAUSE) and p['formulaOracle']==(LEGACY if m==9 else AGG))
        needed_codes=[AGG,AUTO,LEGACY] if m==9 else [AGG,PAUSE,PT_ORACLE]
        require(p['runtimeCodeHashes']=={a:CODE_HASHES[a] for a in needed_codes})
        require(isinstance(p['calls'],list) and 0<len(p['calls'])<=32)
        for c in p['calls']:
            require(c['method'] in ['eth_call','eth_getCode'] and c['params'][1]==tag and re.fullmatch('[0-9a-f]{64}',c['resultHash']) is not None)
        def match(to,name,args,vals):
            params=[{'to':to,'data':'0x'+SEL[name]+''.join(_arg(a) for a in args)},tag]
            raw='0x'+''.join(format(v,'064x') for v in vals)
            digest=hashlib.sha256(json.dumps(raw,separators=(',',':')).encode()).hexdigest()
            require(any(c['method']=='eth_call' and c['params']==params and c['resultHash']==digest for c in p['calls']))
        match(MARGIN,'getMarketPriceOracle',[m],[int(p['directOracle'],16)])
        match(MARGIN,'getMarketTokenAddress',[m],[int(token,16)])
        for a in needed_codes:require(any(c['method']=='eth_getCode' and c['params']==[a,tag] and c['resultHash']==CODE_RESULT_HASHES[a] for c in p['calls']))
        inputs=p['inputs'];glp=uint(inputs['glpQuote']);require(glp>0);match(MARGIN,'getMarketPrice',[6],[glp])
        if m==9:
            assets=uint(inputs['assets']);shares=uint(inputs['shares']);fee=uint(inputs['exitFeeBp']);require(assets>0)
            legacy_fee=[uint(v) for v in inputs['legacyFeeReturn']];active_fee=[uint(v) for v in inputs['activeFeeReturn']]
            require(len(legacy_fee)==len(active_fee)==2 and fee==legacy_fee[0]==active_fee[0])
            require(glp*assets<2**256);price=plutus_formula(glp,assets,shares,fee)
            match(AGG,'getOraclesByToken',[token],[32,1,int(AUTO,16),0,100])
            match(AUTO,'PLUTUS_VAULT_REGISTRY',[],[int(PLV_REGISTRY,16)])
            match(PLV_REGISTRY,'plvGlpToken',[],[int(VAULT,16)]);match(PLV_REGISTRY,'plvGlpRouter',[],[int(ROUTER,16)])
            match(VAULT,'totalAssets',[],[assets]);match(VAULT,'totalSupply',[],[shares])
            match(ROUTER,'getFeeBp',[LEGACY_UNWRAPPER],legacy_fee);match(ROUTER,'getFeeBp',[ACTIVE_UNWRAPPER],active_fee)
        else:
            rate=uint(inputs['ptToAssetRate']);require(rate==10**18 and inputs['deductionCoefficient']=='0' and glp*rate<2**256);price=glp*rate//10**18
            match(PAUSE,'tokenToPaused',[token],[1]);match(AGG,'getOraclesByToken',[token],[32,1,int(PT_ORACLE,16),int(GLP,16),100])
            match(PT_ORACLE,'REGISTRY',[],[int(PT_REGISTRY,16)])
            match(PT_REGISTRY,'ptOracle',[],[int(PT_RATE_ORACLE,16)]);match(PT_REGISTRY,'ptMarket',[],[int(PT_MARKET,16)])
            match(PT_RATE_ORACLE,'getPtToAssetRate',[PT_MARKET,900],[rate]);match(PT_ORACLE,'deductionCoefficient',[],[0]);match(PT_ORACLE,'getPrice',[token],[rate])
        require(price==uint(p['price']) and price>0);match(p['formulaOracle'],'getPrice',[token],[price])
        return price
    except (KeyError,TypeError,ValueError,IndexError,AttributeError,OverflowError) as error:
        if isinstance(error,RecoveryRejected):raise
        raise RecoveryRejected('Malformed reconstruction proof') from error

