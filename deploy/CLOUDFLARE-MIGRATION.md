# Moving the terminal backends behind the Cloudflare edge (HTTPS:443)

Goal: serve `clusters` / `oi-funding` / `journal` / `screener` `.lootx.trade`
through the **Cloudflare proxy (orange-cloud)** so client traffic rides
Cloudflare's anycast `:443/TLS` edge instead of a raw droplet IP (and, for
clusters, instead of plain-HTTP `:8080`). This is more DPI/blocking-resistant,
hides the origin IP, and offloads TLS to the edge. For these request/response
APIs the extra hop is negligible.

The zone SSL/TLS mode is **Full** (not strict) — keep it. Full encrypts
edge↔origin but does not validate the origin cert, which is why a self-signed
origin (clusters) is fine. Do **not** switch the zone to *Full (strict)*: it is
zone-wide and would break any origin without a CF-trusted cert.

## State / what each service needs

| Domain | Origin | Code change | Origin work | Cloudflare |
|---|---|---|---|---|
| clusters.lootx.trade | nginx:443→8080, self-signed, :443 CF-locked (DONE) | done (terminal) | done | enable orange-cloud |
| oi-funding.lootx.trade | Rust+Axum behind nginx (LE), WS ping 30s, IP-agnostic | none | enable orange-cloud (+ optional :443 CF-lock) | enable orange-cloud |
| journal.lootx.trade | Rust+Axum, per-**user** (JWT) rate-limit, no WS | none | enable orange-cloud | enable orange-cloud |
| screener.lootx.trade | Rust+Axum, per-**IP** rate-limit, WS | **DEPLOY** the CF-Connecting-IP fix (commit `b7887b3`) | rebuild+redeploy, then :443 CF-lock | enable orange-cloud |

## 1. Cloudflare dashboard (per domain)

1. **DNS** → the record → set to **Proxied (orange-cloud)**.
2. **SSL/TLS → Overview** → keep **Full** (already set zone-wide).
3. **Security → Bots → Bot Fight Mode = OFF** (zone-wide), *or* add a WAF skip
   rule for these hostnames. The terminal/bot are **non-browser** clients; a JS
   challenge returns HTML instead of JSON and breaks every request. **This is
   the #1 way to break the migration — verify it.**
4. (Optional) **Speed/Caching**: leave default. Responses are `Authorization`-
   gated, so Cloudflare will not cache them.

## 2. Origin hardening (on each droplet — you have SSH there)

Lock public `:443` to Cloudflare ranges so the origin is reachable *only* via
the edge (also what makes trusting `CF-Connecting-IP` safe). UFW example:

```bash
ufw delete allow 443/tcp 2>/dev/null || true
for cidr in $(curl -fsS https://www.cloudflare.com/ips-v4) $(curl -fsS https://www.cloudflare.com/ips-v6); do
  ufw allow from "$cidr" to any port 443 proto tcp comment "Cloudflare edge"
done
ufw reload
```

Refresh these rules if Cloudflare changes its published ranges.

## 3. Per-service notes

- **clusters** — already done by the setup script (`cloudflare-tls-setup.sh`).
  Terminal already points at `https://clusters.lootx.trade` (commit shipped).
  `:8080` stays open for already-installed clients; retire once they roll over.
- **oi-funding** — already behind nginx with a real (LE) cert and a 30s WS
  heartbeat (safe under CF's ~100s idle timeout). No code change. Just enable
  orange-cloud; optionally CF-lock `:443`.
- **journal** — rate-limit is per-user (JWT), so CF's shared IP is irrelevant.
  No WS. No code change.
- **screener** — **MUST deploy commit `b7887b3` first** (reads
  `CF-Connecting-IP`/`X-Forwarded-For`; without it, all clients collapse into
  one CF-IP rate-limit bucket). After deploy + `:443` CF-lock, enable
  orange-cloud. Then **verify the WS survives >100s idle** — if it can sit idle
  (quiet market / no instruments), add a ~30s server-side ping like oi-funding,
  or confirm the client pings.

## 4. Verify (after each flip)

```bash
# returns JSON, NOT an HTML challenge page:
curl -s https://<host>/health -w '\n%{http_code} %{http_version}\n'
curl -s "https://clusters.lootx.trade/v1/clusters/range?exchange=BINANCEF&market_type=perp&symbol=BTCUSDT&interval_seconds=60&from_ms=$(( ($(date +%s)-3600)*1000 ))&to_ms=$(( $(date +%s)*1000 ))" \
     -H "Authorization: Bearer <token>" -w '\n%{http_code}\n' | head -c 200
# DNS now returns Cloudflare IPs (104.* / 172.64-71.*), not the droplet IP:
dig +short <host>
```

If a request returns HTML / a `cf-mitigated` header / 403 → Bot Fight Mode or a
WAF rule is challenging it (step 1.3).

## 5. Rollback

Flip the DNS record back to **DNS-only (grey-cloud)** — instant. (If you locked
`:443` to CF IPs, also re-open it: `ufw allow 443/tcp`, or clients can't reach
the droplet directly.)
