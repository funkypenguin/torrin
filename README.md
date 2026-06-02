# Torrin

Open-source debrid service. Add a magnet link, get a stream.

## What it does

Torrin downloads torrents on remote servers through a VPN and gives you direct HTTPS streaming URLs. No torrent client needed, no IP exposure, no seeding. Content is cached so repeat requests are instant.

## How it works

1. User submits a magnet link or searches for content
2. API checks cache and content providers. If available, returns stream URLs instantly
3. If not available, downloads the torrent behind a VPN
4. On completion, uploads to object storage
5. User streams via signed HTTPS URL

## Features

- Shared cache across all users
- Instant streaming for popular content
- Stremio integration
- Plan-based limits (concurrent slots, max torrent size, priority queue)
- Tiered content retention based on popularity
- Subscription management with pause/resume
- Stall detection with progressive recovery
- Built-in player that handles all formats (MKV, HEVC, AV1, 4K HDR)
- WebDAV server for mounting content in VLC, Infuse, Kodi, or any file manager

## Project structure

```
internal/           # Open-source core (MIT)
  availability/     # Cache checking
  eviction/         # Retention engine
  jobs/             # Job model + store
  poller/           # Download > upload > cleanup pipeline
  qbit/             # Torrent engine client
  r2/               # Object storage client + URL signing
  iptv/             # Content provider client + proxy

private/            # Proprietary (not included)
  cmd/api/          # HTTP API server
  cmd/stremio/      # Stremio addon server
  internal/         # Auth, billing, plans, middleware

web/                # Frontend
worker/             # Streaming CDN worker
```

## Stremio addon

The `comet/` directory contains an open-source Stremio addon (forked from [Comet](https://github.com/g0ldyy/comet)) that works with a self-hosted Torrin instance. It is intended as a **reference design for addon developers** -- not as the primary way to use Torrin with Stremio.

**Casual users:** Use the [ElfHosted](https://elfhosted.com) managed instance. No setup required.

**addon.torrin.app** is Torrin's managed instance of this addon. Self-hosters can deploy their own using the code in `comet/`.

## Self-hosting

Requirements:
- Linux server with Docker
- Object storage with CDN
- VPN provider

```bash
cp .env.example .env
# Fill in your credentials
docker compose up -d
```

## License

MIT. See [LICENSE](LICENSE).
