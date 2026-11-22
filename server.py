"""Read-only crypto research MCP server. Requires: pip install mcp httpx"""
import os
import json
from datetime import datetime, timezone
import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP('Crypto Research Evidence Tools', host='0.0.0.0', port=int(os.environ.get('PORT', '10000')), stateless_http=True, json_response=True)
TIMEOUT = 20.0

async def fetch_json(url: str):
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False, headers={'User-Agent':'CryptoResearchMCP/1.3'}) as c:
        r = await c.get(url)
        r.raise_for_status()
        return r.json()

def wrap(source, payload):
    return {'retrieved_at_utc': datetime.now(timezone.utc).isoformat(), 'source_url': source, 'data': payload, 'verification': 'third_party_api_not_onchain_proof'}

@mcp.tool()
async def protocol_financials(slug: str) -> dict:
    """Read DefiLlama fees/revenue protocol overview by known protocol slug; do not treat fees as revenue."""
    if not slug or not all(c.isalnum() or c in '-_' for c in slug):
        return {'error':'Invalid slug'}
    url = f'https://api.llama.fi/summary/fees/{slug}?dataType=dailyRevenue'
    try:
        data = await fetch_json(url)
        return wrap(url, {'name':data.get('name'), 'totalDataChart':data.get('totalDataChart', [])[-35:], 'total7d':data.get('total7d'), 'total30d':data.get('total30d'), 'note':'Revenue endpoint only; cross-check fee series separately.'})
    except Exception as e:
        return {'error':str(e), 'source_url':url}

@mcp.tool()
async def dex_token_pools(chain: str, token_address: str) -> dict:
    """Get live DEX pairs from DexScreener, filtering strictly by chain and exact token address."""
    if not chain or not token_address or not token_address.startswith('0x') or len(token_address)!=42:
        return {'error':'Expected EVM token address and chain ID'}
    url = f'https://api.dexscreener.com/token-pairs/v1/{chain}/{token_address}'
    try:
        data = await fetch_json(url)
        pairs = [p for p in (data if isinstance(data,list) else []) if p.get('chainId','').lower()==chain.lower() and any((p.get(k) or {}).get('address','').lower()==token_address.lower() for k in ('baseToken','quoteToken'))]
        return wrap(url, [{'pairAddress':p.get('pairAddress'), 'dexId':p.get('dexId'), 'baseToken':p.get('baseToken'), 'quoteToken':p.get('quoteToken'), 'liquidity':p.get('liquidity'), 'volume':p.get('volume'), 'priceUsd':p.get('priceUsd'), 'pairCreatedAt':p.get('pairCreatedAt')} for p in pairs])
    except Exception as e:
        return {'error':str(e), 'source_url':url}

@mcp.tool()
async def verify_evm_transaction(rpc_url: str, tx_hash: str) -> dict:
    """Fetch raw transaction and receipt from a user-provided trusted EVM RPC. This does not alone prove buyback attribution."""
    if not tx_hash.startswith('0x') or len(tx_hash)!=66 or any(c not in '0123456789abcdefABCDEF' for c in tx_hash[2:]):
        return {'error':'Invalid transaction hash'}
    # Prevent untrusted URL access. Set RPC_URL server-side rather than taking arbitrary destinations.
    allowed = os.environ.get('CRYPTO_RPC_URL','')
    if not allowed or rpc_url != allowed or not allowed.startswith('https://'):
        return {'error':'RPC must match server CRYPTO_RPC_URL (HTTPS)'}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as c:
            async def rpc(method):
                r=await c.post(allowed,json={'jsonrpc':'2.0','id':1,'method':method,'params':[tx_hash]})
                r.raise_for_status()
                out=r.json()
                if 'error' in out: raise ValueError(str(out['error']))
                return out.get('result')
            tx=await rpc('eth_getTransactionByHash')
            receipt=await rpc('eth_getTransactionReceipt')
        return {'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'transaction':tx,'receipt':receipt,'verification':'raw_rpc_only; token swaps, ownership, treasury provenance and final destination require independent decoding'}
    except Exception as e:
        return {'error':str(e)}

if __name__ == '__main__':
    mcp.run(transport='streamable-http')
