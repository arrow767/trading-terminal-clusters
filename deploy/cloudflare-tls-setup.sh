#!/usr/bin/env bash
# Reproduce the clusters HTTPS edge on a fresh Ubuntu box.
#
#   nginx :443  ──TLS──>  127.0.0.1:8080 (cluster-ingest REST)
#
# Public TLS is terminated at the Cloudflare edge (orange-cloud); CF connects to
# this origin on :443. The origin cert is a long-lived self-signed pair —
# Cloudflare SSL/TLS mode "Full" does NOT validate it — and :443 is firewalled
# to Cloudflare IP ranges so only the CF edge can reach the origin.
#
# After running this, in the Cloudflare dashboard:
#   1. DNS: set clusters.lootx.trade to Proxied (orange-cloud).
#   2. SSL/TLS: zone mode "Full" (NOT "Full (strict)" — that needs a CF Origin
#      Cert, and the zone is shared with other subdomains).
#   3. Security: ensure Bot Fight Mode is OFF for this hostname (the terminal is
#      a non-browser client and would be JS-challenged otherwise).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export DEBIAN_FRONTEND=noninteractive

apt-get update -qq
apt-get install -y -qq nginx openssl curl

install -d -m 755 /etc/ssl/clusters
if [ ! -f /etc/ssl/clusters/origin.pem ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout /etc/ssl/clusters/origin.key \
    -out    /etc/ssl/clusters/origin.pem \
    -subj   "/CN=clusters.lootx.trade" \
    -addext "subjectAltName=DNS:clusters.lootx.trade"
  chmod 600 /etc/ssl/clusters/origin.key
fi

install -m 644 "$here/nginx/clusters.conf" /etc/nginx/sites-available/clusters.conf
ln -sf /etc/nginx/sites-available/clusters.conf /etc/nginx/sites-enabled/clusters.conf
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl restart nginx

# Lock :443 to Cloudflare edge IPs only (origin reachable solely via CF).
ufw delete allow 443/tcp >/dev/null 2>&1 || true
n=0
for cidr in $(curl -fsS https://www.cloudflare.com/ips-v4) $(curl -fsS https://www.cloudflare.com/ips-v6); do
  ufw allow from "$cidr" to any port 443 proto tcp comment "Cloudflare edge" >/dev/null && n=$((n+1))
done
ufw reload
echo "nginx :443 -> 8080 up; $n Cloudflare CIDR rules on :443."
echo "Verify locally:"
echo "  curl -sk --resolve clusters.lootx.trade:443:127.0.0.1 https://clusters.lootx.trade/health"
