# AGENTS.md - Dashboard Public Assets

## 1. Scope

Policies for the dashboard/public directory, containing static assets served directly by the Next.js application.

## 2. Purpose

The public directory contains static files that are served at the root of the domain without processing. This includes images, fonts, icons, and other assets that need to be referenced directly by URL or embedded in the application.

## 3. Structure

```
public/
├── risingwave.svg           # RisingWave logo (SVG format)
└── (additional static assets)
    ├── fonts/               # Custom font files
    ├── images/              # Image assets
    └── icons/               # Icon files
```

## 4. Key Files

| File | Purpose |
|------|---------|
| `risingwave.svg` | RisingWave brand logo for dashboard header |
| `fonts/` | Custom typography files (if needed) |
| `images/` | Raster image assets |
| `icons/` | Application icons and favicons |

## 5. Edit Rules (Must)

- Use optimized image formats (SVG for logos, WebP for photos)
- Keep file sizes small for fast loading
- Use descriptive, kebab-case filenames
- Organize assets in subdirectories if count exceeds 10
- Reference assets with absolute paths from root (/)
- Include proper alt text when referencing in components
- Optimize SVG files to remove unnecessary metadata
- Provide multiple sizes for responsive images
- Cache-bust assets by versioning filenames
- Add appropriate Cache-Control headers
- Test asset loading in production builds
- Ensure cross-browser compatibility for fonts

## 6. Forbidden Changes (Must Not)

- Add executable files or scripts
- Store sensitive configuration files
- Add unoptimized large binary files
- Use spaces in filenames
- Reference assets with relative paths (../)
- Add copyrighted material without license
- Store generated build files
- Commit placeholder or draft assets

## 7. Test Entry

| Test Type | Entry Command |
|-----------|---------------|
| Build | `cd dashboard && npm run build` |
| Static export | Verify assets in out/ directory |
| Visual check | Confirm assets load in browser |
| Size audit | Check file sizes are optimized |

## 8. Dependencies & Contracts

- Next.js static file serving
- Browser support for file formats used
- CDN configuration for production deployment
- Image optimization pipeline
- Cache invalidation strategy

## 9. Overrides

Inherits from `./dashboard/AGENTS.md`:
- Override: Static asset handling specific to public directory

## 10. Update Triggers

Regenerate this file when:
- Asset organization patterns change
- New static file serving mechanisms are introduced
- Build process changes affecting assets
- New asset types are added

## 11. Metadata

- Created: 2025-02-22
- Version: 1.0
- Parent: ./dashboard/AGENTS.md
