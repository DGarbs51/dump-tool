package cmd

import (
	"fmt"
	"strings"
)

// TableTemplate defines a table's schema for DDL generation.
type TableTemplate struct {
	Name       string
	Tier       int // 0=no FKs, 1-3=increasing dependency depth
	Columns    []ColumnDef
	Indexes    []IndexDef
	ForeignKeys []FKDef
	RowWeight  int // relative data volume weight (higher = more rows)
}

// ColumnDef defines a single column.
type ColumnDef struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
	Unique   bool
}

// IndexDef defines an index.
type IndexDef struct {
	Name    string
	Columns []string
	Unique  bool
}

// FKDef defines a foreign key reference.
type FKDef struct {
	Column     string
	RefTable   string
	RefColumn  string
	OnDelete   string
}

// SelfRefUpdate tracks a self-referential column that needs post-insert UPDATE.
type SelfRefUpdate struct {
	Table  string
	Column string
}

// allTableTemplates returns the full pool of 25 table templates across 4 tiers.
func allTableTemplates() []TableTemplate {
	return []TableTemplate{
		// ── Tier 0: No foreign keys ──
		{
			Name: "users", Tier: 0, RowWeight: 10,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "email", Type: "VARCHAR(255)", Unique: true},
				{Name: "username", Type: "VARCHAR(100)", Unique: true},
				{Name: "password_hash", Type: "VARCHAR(255)"},
				{Name: "first_name", Type: "VARCHAR(100)"},
				{Name: "last_name", Type: "VARCHAR(100)"},
				{Name: "is_active", Type: "BOOLEAN", Default: "true"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			Indexes: []IndexDef{
				{Name: "idx_users_email", Columns: []string{"email"}, Unique: true},
				{Name: "idx_users_username", Columns: []string{"username"}, Unique: true},
				{Name: "idx_users_created_at", Columns: []string{"created_at"}},
			},
		},
		{
			Name: "categories", Tier: 0, RowWeight: 2,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "name", Type: "VARCHAR(200)"},
				{Name: "slug", Type: "VARCHAR(200)", Unique: true},
				{Name: "description", Type: "TEXT", Nullable: true},
				{Name: "parent_id", Type: "INTEGER", Nullable: true},
				{Name: "sort_order", Type: "INTEGER", Default: "0"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			Indexes: []IndexDef{
				{Name: "idx_categories_slug", Columns: []string{"slug"}, Unique: true},
				{Name: "idx_categories_parent_id", Columns: []string{"parent_id"}},
			},
		},
		{
			Name: "tags", Tier: 0, RowWeight: 2,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "name", Type: "VARCHAR(100)"},
				{Name: "slug", Type: "VARCHAR(100)", Unique: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			Indexes: []IndexDef{
				{Name: "idx_tags_slug", Columns: []string{"slug"}, Unique: true},
			},
		},
		{
			Name: "countries", Tier: 0, RowWeight: 1,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "name", Type: "VARCHAR(100)"},
				{Name: "code", Type: "CHAR(2)", Unique: true},
				{Name: "phone_prefix", Type: "VARCHAR(10)", Nullable: true},
			},
			Indexes: []IndexDef{
				{Name: "idx_countries_code", Columns: []string{"code"}, Unique: true},
			},
		},
		{
			Name: "currencies", Tier: 0, RowWeight: 1,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "code", Type: "CHAR(3)", Unique: true},
				{Name: "name", Type: "VARCHAR(100)"},
				{Name: "symbol", Type: "VARCHAR(10)"},
				{Name: "decimal_places", Type: "SMALLINT", Default: "2"},
			},
			Indexes: []IndexDef{
				{Name: "idx_currencies_code", Columns: []string{"code"}, Unique: true},
			},
		},
		{
			Name: "roles", Tier: 0, RowWeight: 1,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "name", Type: "VARCHAR(50)", Unique: true},
				{Name: "description", Type: "TEXT", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			Indexes: []IndexDef{
				{Name: "idx_roles_name", Columns: []string{"name"}, Unique: true},
			},
		},
		{
			Name: "warehouses", Tier: 0, RowWeight: 1,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "name", Type: "VARCHAR(200)"},
				{Name: "code", Type: "VARCHAR(20)", Unique: true},
				{Name: "city", Type: "VARCHAR(100)", Nullable: true},
				{Name: "is_active", Type: "BOOLEAN", Default: "true"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			Indexes: []IndexDef{
				{Name: "idx_warehouses_code", Columns: []string{"code"}, Unique: true},
			},
		},

		// ── Tier 1: Depends on tier 0 ──
		{
			Name: "user_profiles", Tier: 1, RowWeight: 5,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "bio", Type: "TEXT", Nullable: true},
				{Name: "avatar_url", Type: "VARCHAR(500)", Nullable: true},
				{Name: "date_of_birth", Type: "DATE", Nullable: true},
				{Name: "phone", Type: "VARCHAR(30)", Nullable: true},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_user_profiles_user_id", Columns: []string{"user_id"}, Unique: true},
			},
		},
		{
			Name: "user_roles", Tier: 1, RowWeight: 3,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "role_id", Type: "INTEGER"},
				{Name: "assigned_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "role_id", RefTable: "roles", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_user_roles_user_role", Columns: []string{"user_id", "role_id"}, Unique: true},
			},
		},
		{
			Name: "addresses", Tier: 1, RowWeight: 5,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "country_id", Type: "INTEGER"},
				{Name: "line1", Type: "VARCHAR(255)"},
				{Name: "line2", Type: "VARCHAR(255)", Nullable: true},
				{Name: "city", Type: "VARCHAR(100)"},
				{Name: "state", Type: "VARCHAR(100)", Nullable: true},
				{Name: "postal_code", Type: "VARCHAR(20)"},
				{Name: "is_primary", Type: "BOOLEAN", Default: "false"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "country_id", RefTable: "countries", RefColumn: "id", OnDelete: "RESTRICT"},
			},
			Indexes: []IndexDef{
				{Name: "idx_addresses_user_id", Columns: []string{"user_id"}},
				{Name: "idx_addresses_country_id", Columns: []string{"country_id"}},
			},
		},
		{
			Name: "products", Tier: 1, RowWeight: 8,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "category_id", Type: "INTEGER"},
				{Name: "currency_id", Type: "INTEGER"},
				{Name: "name", Type: "VARCHAR(300)"},
				{Name: "slug", Type: "VARCHAR(300)", Unique: true},
				{Name: "description", Type: "TEXT", Nullable: true},
				{Name: "price", Type: "NUMERIC(12,2)"},
				{Name: "cost_price", Type: "NUMERIC(12,2)", Nullable: true},
				{Name: "sku", Type: "VARCHAR(50)", Unique: true},
				{Name: "is_published", Type: "BOOLEAN", Default: "false"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "category_id", RefTable: "categories", RefColumn: "id", OnDelete: "RESTRICT"},
				{Column: "currency_id", RefTable: "currencies", RefColumn: "id", OnDelete: "RESTRICT"},
			},
			Indexes: []IndexDef{
				{Name: "idx_products_slug", Columns: []string{"slug"}, Unique: true},
				{Name: "idx_products_sku", Columns: []string{"sku"}, Unique: true},
				{Name: "idx_products_category_id", Columns: []string{"category_id"}},
				{Name: "idx_products_is_published", Columns: []string{"is_published"}},
			},
		},
		{
			Name: "posts", Tier: 1, RowWeight: 6,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "title", Type: "VARCHAR(500)"},
				{Name: "slug", Type: "VARCHAR(500)", Unique: true},
				{Name: "body", Type: "TEXT"},
				{Name: "status", Type: "VARCHAR(20)", Default: "'draft'"},
				{Name: "published_at", Type: "TIMESTAMP", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_posts_slug", Columns: []string{"slug"}, Unique: true},
				{Name: "idx_posts_user_id", Columns: []string{"user_id"}},
				{Name: "idx_posts_status", Columns: []string{"status"}},
				{Name: "idx_posts_published_at", Columns: []string{"published_at"}},
			},
		},
		{
			Name: "sessions", Tier: 1, RowWeight: 8,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "token", Type: "VARCHAR(255)", Unique: true},
				{Name: "ip_address", Type: "VARCHAR(45)", Nullable: true},
				{Name: "user_agent", Type: "TEXT", Nullable: true},
				{Name: "last_activity", Type: "TIMESTAMP", Default: "NOW()"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_sessions_token", Columns: []string{"token"}, Unique: true},
				{Name: "idx_sessions_user_id", Columns: []string{"user_id"}},
				{Name: "idx_sessions_last_activity", Columns: []string{"last_activity"}},
			},
		},
		{
			Name: "audit_logs", Tier: 1, RowWeight: 12,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER", Nullable: true},
				{Name: "action", Type: "VARCHAR(50)"},
				{Name: "entity_type", Type: "VARCHAR(100)"},
				{Name: "entity_id", Type: "INTEGER"},
				{Name: "old_values", Type: "TEXT", Nullable: true},
				{Name: "new_values", Type: "TEXT", Nullable: true},
				{Name: "ip_address", Type: "VARCHAR(45)", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "SET NULL"},
			},
			Indexes: []IndexDef{
				{Name: "idx_audit_logs_user_id", Columns: []string{"user_id"}},
				{Name: "idx_audit_logs_entity", Columns: []string{"entity_type", "entity_id"}},
				{Name: "idx_audit_logs_created_at", Columns: []string{"created_at"}},
			},
		},
		{
			Name: "notifications", Tier: 1, RowWeight: 10,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "type", Type: "VARCHAR(100)"},
				{Name: "title", Type: "VARCHAR(255)"},
				{Name: "body", Type: "TEXT", Nullable: true},
				{Name: "is_read", Type: "BOOLEAN", Default: "false"},
				{Name: "read_at", Type: "TIMESTAMP", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_notifications_user_id", Columns: []string{"user_id"}},
				{Name: "idx_notifications_is_read", Columns: []string{"user_id", "is_read"}},
				{Name: "idx_notifications_created_at", Columns: []string{"created_at"}},
			},
		},

		// ── Tier 2: Depends on tier 0 + 1 ──
		{
			Name: "orders", Tier: 2, RowWeight: 7,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "currency_id", Type: "INTEGER"},
				{Name: "address_id", Type: "INTEGER", Nullable: true},
				{Name: "order_number", Type: "VARCHAR(50)", Unique: true},
				{Name: "status", Type: "VARCHAR(30)", Default: "'pending'"},
				{Name: "subtotal", Type: "NUMERIC(12,2)"},
				{Name: "tax", Type: "NUMERIC(12,2)", Default: "0"},
				{Name: "total", Type: "NUMERIC(12,2)"},
				{Name: "notes", Type: "TEXT", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "RESTRICT"},
				{Column: "currency_id", RefTable: "currencies", RefColumn: "id", OnDelete: "RESTRICT"},
				{Column: "address_id", RefTable: "addresses", RefColumn: "id", OnDelete: "SET NULL"},
			},
			Indexes: []IndexDef{
				{Name: "idx_orders_order_number", Columns: []string{"order_number"}, Unique: true},
				{Name: "idx_orders_user_id", Columns: []string{"user_id"}},
				{Name: "idx_orders_status", Columns: []string{"status"}},
				{Name: "idx_orders_created_at", Columns: []string{"created_at"}},
			},
		},
		{
			Name: "product_tags", Tier: 2, RowWeight: 4,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "product_id", Type: "INTEGER"},
				{Name: "tag_id", Type: "INTEGER"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "product_id", RefTable: "products", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "tag_id", RefTable: "tags", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_product_tags_product_tag", Columns: []string{"product_id", "tag_id"}, Unique: true},
			},
		},
		{
			Name: "product_images", Tier: 2, RowWeight: 5,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "product_id", Type: "INTEGER"},
				{Name: "url", Type: "VARCHAR(500)"},
				{Name: "alt_text", Type: "VARCHAR(255)", Nullable: true},
				{Name: "sort_order", Type: "INTEGER", Default: "0"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "product_id", RefTable: "products", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_product_images_product_id", Columns: []string{"product_id"}},
			},
		},
		{
			Name: "inventory", Tier: 2, RowWeight: 4,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "product_id", Type: "INTEGER"},
				{Name: "warehouse_id", Type: "INTEGER"},
				{Name: "quantity", Type: "INTEGER", Default: "0"},
				{Name: "reserved", Type: "INTEGER", Default: "0"},
				{Name: "reorder_level", Type: "INTEGER", Default: "10"},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "product_id", RefTable: "products", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "warehouse_id", RefTable: "warehouses", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_inventory_product_warehouse", Columns: []string{"product_id", "warehouse_id"}, Unique: true},
			},
		},
		{
			Name: "comments", Tier: 2, RowWeight: 8,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "post_id", Type: "INTEGER"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "parent_id", Type: "INTEGER", Nullable: true},
				{Name: "body", Type: "TEXT"},
				{Name: "is_approved", Type: "BOOLEAN", Default: "true"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
				{Name: "updated_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "post_id", RefTable: "posts", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_comments_post_id", Columns: []string{"post_id"}},
				{Name: "idx_comments_user_id", Columns: []string{"user_id"}},
				{Name: "idx_comments_parent_id", Columns: []string{"parent_id"}},
			},
		},
		{
			Name: "post_tags", Tier: 2, RowWeight: 4,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "post_id", Type: "INTEGER"},
				{Name: "tag_id", Type: "INTEGER"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "post_id", RefTable: "posts", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "tag_id", RefTable: "tags", RefColumn: "id", OnDelete: "CASCADE"},
			},
			Indexes: []IndexDef{
				{Name: "idx_post_tags_post_tag", Columns: []string{"post_id", "tag_id"}, Unique: true},
			},
		},

		// ── Tier 3: Depends on tier 2 ──
		{
			Name: "order_items", Tier: 3, RowWeight: 10,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "order_id", Type: "INTEGER"},
				{Name: "product_id", Type: "INTEGER"},
				{Name: "quantity", Type: "INTEGER"},
				{Name: "unit_price", Type: "NUMERIC(12,2)"},
				{Name: "total_price", Type: "NUMERIC(12,2)"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "order_id", RefTable: "orders", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "product_id", RefTable: "products", RefColumn: "id", OnDelete: "RESTRICT"},
			},
			Indexes: []IndexDef{
				{Name: "idx_order_items_order_id", Columns: []string{"order_id"}},
				{Name: "idx_order_items_product_id", Columns: []string{"product_id"}},
			},
		},
		{
			Name: "payments", Tier: 3, RowWeight: 5,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "order_id", Type: "INTEGER"},
				{Name: "amount", Type: "NUMERIC(12,2)"},
				{Name: "currency_id", Type: "INTEGER"},
				{Name: "method", Type: "VARCHAR(30)"},
				{Name: "status", Type: "VARCHAR(30)", Default: "'pending'"},
				{Name: "transaction_id", Type: "VARCHAR(255)", Nullable: true},
				{Name: "paid_at", Type: "TIMESTAMP", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "order_id", RefTable: "orders", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "currency_id", RefTable: "currencies", RefColumn: "id", OnDelete: "RESTRICT"},
			},
			Indexes: []IndexDef{
				{Name: "idx_payments_order_id", Columns: []string{"order_id"}},
				{Name: "idx_payments_status", Columns: []string{"status"}},
				{Name: "idx_payments_transaction_id", Columns: []string{"transaction_id"}},
			},
		},
		{
			Name: "reviews", Tier: 3, RowWeight: 6,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "product_id", Type: "INTEGER"},
				{Name: "user_id", Type: "INTEGER"},
				{Name: "order_id", Type: "INTEGER", Nullable: true},
				{Name: "rating", Type: "SMALLINT"},
				{Name: "title", Type: "VARCHAR(255)", Nullable: true},
				{Name: "body", Type: "TEXT", Nullable: true},
				{Name: "is_verified", Type: "BOOLEAN", Default: "false"},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "product_id", RefTable: "products", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "user_id", RefTable: "users", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "order_id", RefTable: "orders", RefColumn: "id", OnDelete: "SET NULL"},
			},
			Indexes: []IndexDef{
				{Name: "idx_reviews_product_id", Columns: []string{"product_id"}},
				{Name: "idx_reviews_user_id", Columns: []string{"user_id"}},
				{Name: "idx_reviews_rating", Columns: []string{"product_id", "rating"}},
			},
		},
		{
			Name: "shipments", Tier: 3, RowWeight: 4,
			Columns: []ColumnDef{
				{Name: "id", Type: "SERIAL PRIMARY KEY"},
				{Name: "order_id", Type: "INTEGER"},
				{Name: "address_id", Type: "INTEGER", Nullable: true},
				{Name: "tracking_number", Type: "VARCHAR(100)", Nullable: true},
				{Name: "carrier", Type: "VARCHAR(50)", Nullable: true},
				{Name: "status", Type: "VARCHAR(30)", Default: "'processing'"},
				{Name: "shipped_at", Type: "TIMESTAMP", Nullable: true},
				{Name: "delivered_at", Type: "TIMESTAMP", Nullable: true},
				{Name: "created_at", Type: "TIMESTAMP", Default: "NOW()"},
			},
			ForeignKeys: []FKDef{
				{Column: "order_id", RefTable: "orders", RefColumn: "id", OnDelete: "CASCADE"},
				{Column: "address_id", RefTable: "addresses", RefColumn: "id", OnDelete: "SET NULL"},
			},
			Indexes: []IndexDef{
				{Name: "idx_shipments_order_id", Columns: []string{"order_id"}},
				{Name: "idx_shipments_tracking_number", Columns: []string{"tracking_number"}},
				{Name: "idx_shipments_status", Columns: []string{"status"}},
			},
		},
	}
}

// selfRefColumns returns the self-referential columns that need post-insert UPDATEs.
func selfRefColumns() []SelfRefUpdate {
	return []SelfRefUpdate{
		{Table: "categories", Column: "parent_id"},
		{Table: "comments", Column: "parent_id"},
	}
}

// selectTables picks targetCount tables from the pool, expanding FK dependencies,
// and returns them in topological (creation) order.
func selectTables(pool []TableTemplate, targetCount int) []TableTemplate {
	if targetCount >= len(pool) {
		return topologicalSort(pool)
	}

	byName := make(map[string]TableTemplate, len(pool))
	for _, t := range pool {
		byName[t.Name] = t
	}

	selected := make(map[string]bool)

	// Always include at least one tier-0 table (users is foundational)
	selected["users"] = true

	// Random pick from pool until we reach targetCount
	poolCopy := make([]TableTemplate, len(pool))
	copy(poolCopy, pool)
	// Shuffle using our existing randIntRange
	for i := len(poolCopy) - 1; i > 0; i-- {
		j, err := randIntRange(0, i)
		if err != nil {
			j = 0
		}
		poolCopy[i], poolCopy[j] = poolCopy[j], poolCopy[i]
	}

	for _, t := range poolCopy {
		if len(selected) >= targetCount {
			break
		}
		selected[t.Name] = true
	}

	// Expand FK dependencies recursively
	changed := true
	for changed {
		changed = false
		for name := range selected {
			tmpl, ok := byName[name]
			if !ok {
				continue
			}
			for _, fk := range tmpl.ForeignKeys {
				if !selected[fk.RefTable] {
					selected[fk.RefTable] = true
					changed = true
				}
			}
		}
	}

	// Build result slice
	var result []TableTemplate
	for _, t := range pool {
		if selected[t.Name] {
			result = append(result, t)
		}
	}

	return topologicalSort(result)
}

// topologicalSort returns tables in dependency order using Kahn's algorithm.
func topologicalSort(tables []TableTemplate) []TableTemplate {
	nameSet := make(map[string]bool, len(tables))
	byName := make(map[string]TableTemplate, len(tables))
	for _, t := range tables {
		nameSet[t.Name] = true
		byName[t.Name] = t
	}

	// Build adjacency: inDegree[child] = count of parent deps within the set
	inDegree := make(map[string]int, len(tables))
	dependents := make(map[string][]string) // parent -> children
	for _, t := range tables {
		if _, ok := inDegree[t.Name]; !ok {
			inDegree[t.Name] = 0
		}
		for _, fk := range t.ForeignKeys {
			if nameSet[fk.RefTable] && fk.RefTable != t.Name {
				inDegree[t.Name]++
				dependents[fk.RefTable] = append(dependents[fk.RefTable], t.Name)
			}
		}
	}

	// Kahn's algorithm
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var sorted []TableTemplate
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		sorted = append(sorted, byName[name])
		for _, child := range dependents[name] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	// If cycle detected (shouldn't happen), append remaining
	if len(sorted) < len(tables) {
		for _, t := range tables {
			found := false
			for _, s := range sorted {
				if s.Name == t.Name {
					found = true
					break
				}
			}
			if !found {
				sorted = append(sorted, t)
			}
		}
	}

	return sorted
}

// generateDDL produces the CREATE TABLE and CREATE INDEX statements for a template.
// The dbPrefix is used to namespace index names to avoid collisions across databases.
func generateDDL(tmpl TableTemplate, dbPrefix string) []string {
	var stmts []string

	// CREATE TABLE
	var cols []string
	for _, c := range tmpl.Columns {
		col := fmt.Sprintf("  %s %s", pgIdentifier(c.Name), c.Type)
		if c.Unique && !strings.Contains(strings.ToUpper(c.Type), "PRIMARY KEY") {
			col += " UNIQUE"
		}
		if !c.Nullable && c.Name != "id" && !strings.Contains(strings.ToUpper(c.Type), "PRIMARY KEY") {
			col += " NOT NULL"
		}
		if c.Nullable {
			// NULL is default, but be explicit for clarity in DDL
		}
		if c.Default != "" {
			col += " DEFAULT " + c.Default
		}
		cols = append(cols, col)
	}

	// FK constraints (skip self-referential ones — handled via post-insert UPDATE)
	selfRefs := selfRefColumns()
	isSelfRef := func(table, column string) bool {
		for _, sr := range selfRefs {
			if sr.Table == table && sr.Column == column {
				return true
			}
		}
		return false
	}

	for _, fk := range tmpl.ForeignKeys {
		if isSelfRef(tmpl.Name, fk.Column) {
			continue
		}
		onDelete := "RESTRICT"
		if fk.OnDelete != "" {
			onDelete = fk.OnDelete
		}
		constraint := fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s) ON DELETE %s",
			pgIdentifier(fmt.Sprintf("fk_%s_%s", tmpl.Name, fk.Column)),
			pgIdentifier(fk.Column),
			pgIdentifier(fk.RefTable),
			pgIdentifier(fk.RefColumn),
			onDelete,
		)
		cols = append(cols, constraint)
	}

	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)",
		pgIdentifier(tmpl.Name),
		strings.Join(cols, ",\n"),
	)
	stmts = append(stmts, createTable)

	// CREATE INDEX statements
	for _, idx := range tmpl.Indexes {
		idxName := dbPrefix + "_" + idx.Name
		var quotedCols []string
		for _, c := range idx.Columns {
			quotedCols = append(quotedCols, pgIdentifier(c))
		}
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		stmt := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
			unique,
			pgIdentifier(idxName),
			pgIdentifier(tmpl.Name),
			strings.Join(quotedCols, ", "),
		)
		stmts = append(stmts, stmt)
	}

	return stmts
}
