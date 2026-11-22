"Read-only crypto research MCP server (V1.3.1). Requires mcp>=1,<2 and httpx>=0.27,<1."
import asyncio
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    'Crypto Research Evidence Tools',
    host='0.0.0.0',
    port=int(os.environ.get('PORT', '10000')),
    stateless_http=True,
    json_response=True,
)
TIMEOUT = 20.0
_CACHE = {}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def wrap(source, payload, *, cached=False):
    return {
        'retrieved_at_utc': utc_now(),
        'source_url': source,
        'cached': cached,
        'data': payload,
        'verification': 'third_party_api_not_onchain_proof',
    }


async def fetch_json(url: str, *, cache_seconds: int = 0):
    now = time.monotonic()
    cached = _CACHE.get(url)
    if cached and cached[0] > now:
        return cached[1], True

    headers = {'User-Agent': 'CryptoResearchMCP/1.3.1', 'Accept': 'application/json'}
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False, headers=headers) as client:
        for attempt in range(3):
            response = await client.get(url)
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                retry_after = response.headers.get('Retry-After', '')
                try:
                    delay = min(max(float(retry_after), 0.5), 5.0)
                except ValueError:
                    delay = float(attempt + 1)
                await asyncio.sleep(delay)
                continue
            response.raise_for_status()
            data = response.json()
            if cache_seconds:
                _CACHE[url] = (time.monotonic() + cache_seconds, data)
            return data, False
    raise RuntimeError('Unexpected retry state')


def api_error(exc, url):
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        hints = {
            400: 'API rejected the request; verify the endpoint, dataType and exact protocol slug.',
            404: 'API resource not found; verify the exact protocol slug or chain identifier.',
            429: 'API rate limited the request; retry later. No zero value is implied.',
        }
        return {
            'error_type': 'upstream_http_error',
            'status_code': status,
            'error': hints.get(status, f'Upstream HTTP {status}'),
            'source_url': url,
            'retrieved_at_utc': utc_now(),
            'verified': False,
        }
    return {
        'error_type': 'upstream_request_error',
        'error': f'{type(exc).__name__}: {exc}',
        'source_url': url,
        'retrieved_at_utc': utc_now(),
        'verified': False,
    }


@mcp.tool()
async def protocol_financials(slug: str) -> dict:
    """Read DefiLlama protocol revenue/fees, with explicit source and missing-data handling. Never confuse fees with revenue."""
    if not isinstance(slug, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', slug):
        return {'error': 'Invalid protocol slug'}

    # DefiLlama's fees API distinguishes protocol slug and dataType.
    # Some projects are not present under the guessed slug: do not invent values.
    slug = quote(slug.lower(), safe='-_')
    base = f'https://api.llama.fi/summary/fees/{slug}'
    output = {}
    failures = {}
    for label, data_type in (('revenue', 'dailyRevenue'), ('fees', 'dailyFees')):
        url = f'{base}?dataType={data_type}'
        try:
            data, cached = await fetch_json(url, cache_seconds=120)
            output[label] = {
                'source_url': url,
                'cached': cached,
                'name': data.get('name'),
                'total7d': data.get('total7d'),
                'total30d': data.get('total30d'),
                'totalDataChart_last35': (data.get('totalDataChart') or [])[-35:],
            }
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            failures[label] = api_error(exc, url)

    if not output:
        return {
            'error': 'No DefiLlama fees/revenue data could be verified for this slug.',
            'slug': slug,
            'failures': failures,
            'note': 'A 400/404 is not evidence of zero revenue. Confirm the protocol slug from DefiLlama.',
        }
    return {
        'retrieved_at_utc': utc_now(),
        'slug': slug,
        'data': output,
        'failures': failures,
        'verification': 'third_party_api_not_onchain_proof',
        'note': 'Fees and revenue are separate metrics. Missing metrics must not be treated as zero.',
    }


@mcp.tool()
async def dex_token_pools(chain: str, token_address: str) -> dict:
    """Get live DexScreener token pools, strictly matching chain and EVM token address; retry temporary rate limits."""
    if not isinstance(chain, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,40}', chain):
        return {'error': 'Invalid chain identifier'}
    if not isinstance(token_address, str) or not re.fullmatch(r'0x[0-9a-fA-F]{40}', token_address):
        return {'error': 'Expected a 42-character EVM token address'}

    url = f'https://api.dexscreener.com/token-pairs/v1/{quote(chain.lower(), safe="-_")}/{token_address}'
    try:
        data, cached = await fetch_json(url, cache_seconds=90)
        if not isinstance(data, list):
            return {'error': 'Unexpected DexScreener response shape', 'source_url': url}
        pairs = [p for p in data if isinstance(p, dict)
                 and str(p.get('chainId', '')).lower() == chain.lower()
                 and any(str((p.get(k) or {}).get('address', '')).lower() == token_address.lower()
                         for k in ('baseToken', 'quoteToken'))]
        result = [{
            'pairAddress': p.get('pairAddress'),
            'dexId': p.get('dexId'),
            'baseToken': p.get('baseToken'),
            'quoteToken': p.get('quoteToken'),
            'liquidity': p.get('liquidity'),
            'volume': p.get('volume'),
            'priceUsd': p.get('priceUsd'),
            'pairCreatedAt': p.get('pairCreatedAt'),
        } for p in pairs]
        return wrap(url, {
            'chain': chain,
            'token_address': token_address,
            'pair_count': len(result),
            'pairs': result,
            'note': 'Snapshot only. No historical 7d/30d liquidity delta or top-wallet flows are inferred.',
        }, cached=cached)
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        return api_error(exc, url)


@mcp.tool()
async def verify_evm_transaction(rpc_url: str, tx_hash: str) -> dict:
    """Read an EVM transaction and receipt from the server-allowlisted RPC; this alone does NOT prove a buyback."""
    if not isinstance(tx_hash, str) or not re.fullmatch(r'0x[0-9a-fA-F]{64}', tx_hash):
        return {'error': 'Invalid transaction hash'}
    allowed = os.environ.get('CRYPTO_RPC_URL', '')
    if not allowed or not allowed.startswith('https://') or rpc_url != allowed:
        return {'error': 'RPC must match server CRYPTO_RPC_URL (HTTPS)'}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=False) as client:
            async def rpc(method):
                response = await client.post(allowed, json={
                    'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': [tx_hash],
                })
                response.raise_for_status()
                payload = response.json()
                if payload.get('error'):
                    raise ValueError(str(payload['error']))
                return payload.get('result')

            tx = await rpc('eth_getTransactionByHash')
            receipt = await rpc('eth_getTransactionReceipt')
        return {
            'retrieved_at_utc': utc_now(),
            'transaction': tx,
            'receipt': receipt,
            'verification': 'raw_rpc_only_not_buyback_proof',
            'note': 'To prove a buyback, independently verify treasury source, swap logs, exact token contract, destination, and burn/lock status.',
        }
    except (httpx.HTTPError, ValueError, TypeError) as exc:
        return api_error(exc, 'server-configured CRYPTO_RPC_URL')


if __name__ == '__main__':
    mcp.run(transport='streamable-http')
