export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  public: {
    Tables: {
      categories: {
        Row: {
          category_id: number
          description: string | null
          name: string
        }
        Insert: {
          category_id?: number
          description?: string | null
          name: string
        }
        Update: {
          category_id?: number
          description?: string | null
          name?: string
        }
        Relationships: []
      }
      ingredients: {
        Row: {
          category_id: number | null
          ingredient_id: number
          name: string
          status: string | null
        }
        Insert: {
          category_id?: number | null
          ingredient_id?: number
          name: string
          status?: string | null
        }
        Update: {
          category_id?: number | null
          ingredient_id?: number
          name?: string
          status?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "ingredients_category_id_fkey"
            columns: ["category_id"]
            isOneToOne: false
            referencedRelation: "categories"
            referencedColumns: ["category_id"]
          },
        ]
      }
      ingredientvariants: {
        Row: {
          ingredient_id: number
          manufacturer_id: number | null
          status: string | null
          variant_id: number
          variant_name: string
        }
        Insert: {
          ingredient_id: number
          manufacturer_id?: number | null
          status?: string | null
          variant_id?: number
          variant_name: string
        }
        Update: {
          ingredient_id?: number
          manufacturer_id?: number | null
          status?: string | null
          variant_id?: number
          variant_name?: string
        }
        Relationships: [
          {
            foreignKeyName: "ingredientvariants_ingredient_id_fkey"
            columns: ["ingredient_id"]
            isOneToOne: false
            referencedRelation: "ingredients"
            referencedColumns: ["ingredient_id"]
          },
          {
            foreignKeyName: "ingredientvariants_manufacturer_id_fkey"
            columns: ["manufacturer_id"]
            isOneToOne: false
            referencedRelation: "manufacturers"
            referencedColumns: ["manufacturer_id"]
          },
        ]
      }
      pricetiers: {
        Row: {
          discount_percent: number | null
          effective_date: string
          includes_shipping: number | null
          min_quantity: number | null
          original_price: number | null
          price: number
          price_per_kg: number | null
          price_tier_id: number
          pricing_model_id: number
          source_id: number | null
          unit_id: number | null
          vendor_ingredient_id: number
        }
        Insert: {
          discount_percent?: number | null
          effective_date: string
          includes_shipping?: number | null
          min_quantity?: number | null
          original_price?: number | null
          price: number
          price_per_kg?: number | null
          price_tier_id?: number
          pricing_model_id: number
          source_id?: number | null
          unit_id?: number | null
          vendor_ingredient_id: number
        }
        Update: {
          discount_percent?: number | null
          effective_date?: string
          includes_shipping?: number | null
          min_quantity?: number | null
          original_price?: number | null
          price?: number
          price_per_kg?: number | null
          price_tier_id?: number
          pricing_model_id?: number
          source_id?: number | null
          unit_id?: number | null
          vendor_ingredient_id?: number
        }
        Relationships: []
      }
      vendoringredients: {
        Row: {
          barcode: string | null
          current_source_id: number | null
          last_seen_at: string | null
          raw_product_name: string | null
          shipping_responsibility: string | null
          shipping_terms: string | null
          sku: string | null
          status: string | null
          variant_id: number
          vendor_id: number
          vendor_ingredient_id: number
        }
        Insert: {
          barcode?: string | null
          current_source_id?: number | null
          last_seen_at?: string | null
          raw_product_name?: string | null
          shipping_responsibility?: string | null
          shipping_terms?: string | null
          sku?: string | null
          status?: string | null
          variant_id: number
          vendor_id: number
          vendor_ingredient_id?: number
        }
        Update: {
          barcode?: string | null
          current_source_id?: number | null
          last_seen_at?: string | null
          raw_product_name?: string | null
          shipping_responsibility?: string | null
          shipping_terms?: string | null
          sku?: string | null
          status?: string | null
          variant_id?: number
          vendor_id?: number
          vendor_ingredient_id?: number
        }
        Relationships: []
      }
      vendors: {
        Row: {
          name: string
          pricing_model: string | null
          status: string | null
          vendor_id: number
        }
        Insert: {
          name: string
          pricing_model?: string | null
          status?: string | null
          vendor_id?: number
        }
        Update: {
          name?: string
          pricing_model?: string | null
          status?: string | null
          vendor_id?: number
        }
        Relationships: []
      }
      scrapesources: {
        Row: {
          product_url: string
          scraped_at: string
          source_id: number
          vendor_id: number
        }
        Insert: {
          product_url: string
          scraped_at: string
          source_id?: number
          vendor_id: number
        }
        Update: {
          product_url?: string
          scraped_at?: string
          source_id?: number
          vendor_id?: number
        }
        Relationships: []
      }
      packagingsizes: {
        Row: {
          description: string | null
          package_id: number
          quantity: number
          unit_id: number | null
          vendor_ingredient_id: number
        }
        Insert: {
          description?: string | null
          package_id?: number
          quantity: number
          unit_id?: number | null
          vendor_ingredient_id: number
        }
        Update: {
          description?: string | null
          package_id?: number
          quantity?: number
          unit_id?: number | null
          vendor_ingredient_id?: number
        }
        Relationships: []
      }
      units: {
        Row: {
          base_unit: string
          conversion_factor: number
          name: string
          type: string
          unit_id: number
        }
        Insert: {
          base_unit: string
          conversion_factor: number
          name: string
          type: string
          unit_id?: number
        }
        Update: {
          base_unit?: string
          conversion_factor?: number
          name?: string
          type?: string
          unit_id?: number
        }
        Relationships: []
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      [_ in never]: never
    }
    Enums: {
      [_ in never]: never
    }
  }
}

export type Tables<T extends keyof Database['public']['Tables']> = Database['public']['Tables'][T]['Row']
export type Vendor = Tables<'vendors'>
export type Ingredient = Tables<'ingredients'>
export type VendorIngredient = Tables<'vendoringredients'>
export type PriceTier = Tables<'pricetiers'>
export type ScrapeSource = Tables<'scrapesources'>
export type PackagingSize = Tables<'packagingsizes'>
export type Unit = Tables<'units'>
