# Sitemap Generator

A standalone Go tool to generate sitemaps in multiple formats while respecting robots.txt rules.

## Features

- **Respects robots.txt** - Follows Disallow/Allow rules and crawl-delay
- **Same-domain only** - Only crawls URLs within the specified domain (no subdomains)
- **Concurrent crawling** - Uses 5 worker goroutines for faster crawling
- **Graceful shutdown** - Handles Ctrl+C interrupts cleanly
- **HTML only** - Only processes HTML pages, skips other content types
- **Multiple formats** - XML, TXT, and JSON output options

## Usage

```bash
./generateSiteMap <domain> [format] [delay]
```

**Parameters:**
- `domain` - Domain to crawl (required)
- `format` - Output format: xml, txt, json (default: xml)
- `delay` - Seconds between requests (2-99, default: 30)

**Formats:**
- `xml` (default) - Standard XML sitemap
- `txt` - Plain text list of URLs  
- `json` - JSON format with metadata

## Examples

```bash
# XML sitemap with 30s delay (default)
./generateSiteMap example.com

# Text sitemap with 20s delay
./generateSiteMap example.com txt 20

# JSON sitemap with 2s delay (minimum)
./generateSiteMap example.com json 2

# Delay validation: values <2 become 2, >99 become 99
./generateSiteMap example.com txt 1    # Uses 2s minimum
./generateSiteMap example.com txt 150  # Uses 99s maximum
```

## Output

Generates sitemap files based on format:

- **XML**: `<domain>.xml` - Standard sitemap format
- **TXT**: `<domain>.txt` - One URL per line
- **JSON**: `<domain>.json` - Structured data with metadata

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://example.com/</loc>
    <lastmod>2026-03-01</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>
  <!-- more URLs... -->
</urlset>
```

**JSON Example:**
```json
{
  "domain": "example.com",
  "urls": [
    "https://example.com/",
    "https://example.com/about",
    "https://example.com/contact"
  ],
  "count": 3
}
```

## Behavior

1. **Fetches robots.txt** from `https://<domain>/robots.txt`
2. **Parses rules** for `*` or `SitemapGenerator` user-agent
3. **Applies delay** between requests (default: 30s, customizable 2-99s)
4. **Crawls HTML pages** respecting Disallow/Allow rules
5. **Follows only same-domain links** (no subdomains or external domains)
6. **15-minute timeout** for large sites (prevents infinite hanging)
7. **Generates sitemap** with all discovered URLs in specified format

If robots.txt is not found, it continues with no restrictions. The 15-minute timeout ensures completion even for sites with thousands of pages.

## Build

```bash
cd sitemap-generator
go build -o generateSiteMap .
```
