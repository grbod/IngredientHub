# Frontend Implementation Plan

## Decision Summary
- **Framework:** Vite + React (not Next.js - no SSR needed for internal tool)
- **Styling:** Tailwind CSS + shadcn/ui
- **Data:** Direct Supabase JS client from frontend

## Tech Stack

| Layer | Choice |
|-------|--------|
| Build | Vite |
| Framework | React 18 + TypeScript |
| Styling | Tailwind CSS |
| Components | shadcn/ui |
| Data fetching | @tanstack/react-query |
| Tables | @tanstack/react-table (shadcn data-table) |
| Database | Supabase JS client |
| Routing | React Router (if needed) |

## Project Structure

```
/frontend/
├── src/
│   ├── components/
│   │   └── ui/           # shadcn components
│   ├── lib/
│   │   └── supabase.ts   # supabase client
│   ├── hooks/            # react-query hooks
│   ├── pages/            # page components
│   └── App.tsx
├── .env                  # VITE_SUPABASE_URL, VITE_SUPABASE_ANON_KEY
├── tailwind.config.js
└── vite.config.ts
```

## Environment Variables

```bash
# frontend/.env
VITE_SUPABASE_URL=https://xxx.supabase.co
VITE_SUPABASE_ANON_KEY=eyJ...  # Public anon key (safe for frontend)
```

## Key Views to Build

1. **Dashboard** - Overview of all vendors, last scrape times, product counts
2. **Products Table** - Filterable/sortable across vendors (IO, BS, BN, TP)
3. **Price Comparison** - Same ingredient across vendors
4. **Scrape History/Logs** (optional)

## Setup Commands

```bash
# From project root
npm create vite@latest frontend -- --template react-ts
cd frontend

# Install dependencies
npm install @tanstack/react-query @tanstack/react-table @supabase/supabase-js

# Initialize Tailwind
npm install -D tailwindcss postcss autoprefixer
npx tailwindcss init -p

# Initialize shadcn/ui
npx shadcn@latest init

# Add shadcn components as needed
npx shadcn@latest add button
npx shadcn@latest add table
npx shadcn@latest add input
npx shadcn@latest add data-table
```

## Database Tables to Query

From Supabase (existing tables):
- `BSPricing` - BulkSupplements data
- `BoxNutraPricing` - BoxNutra data
- `TrafaPricing` - TrafaPharma data
- `Pricing` - IngredientsOnline data
- `Vendors` - Vendor metadata
- `Ingredients` - Ingredient names

## Notes

- Internal tool only (no SEO needed)
- Backend scrapers handle all data collection
- Frontend is read-only display of scraped data
- Free shipping threshold: BoxNutra $49+, BulkSupplements always free
