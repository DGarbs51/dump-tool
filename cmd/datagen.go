package cmd

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	crand "crypto/rand"
	mrand "math/rand"
)

// ── Word pools ──

var firstNames = []string{
	"James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
	"David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
	"Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
	"Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
	"Steven", "Dorothy", "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna",
	"Kenneth", "Michelle", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
	"Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
	"Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
	"Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
	"Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
	"Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
	"Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
	"Dennis", "Maria", "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Ruth",
	"Jose", "Julie", "Adam", "Olivia", "Nathan", "Joyce", "Henry", "Virginia",
	"Peter", "Victoria", "Zachary", "Kelly", "Douglas", "Lauren", "Harold", "Christina",
	"Albert", "Joan", "Carl", "Evelyn", "Arthur", "Judith", "Gerald", "Megan",
	"Roger", "Andrea", "Keith", "Cheryl", "Jeremy", "Hannah", "Terry", "Jacqueline",
	"Lawrence", "Martha", "Sean", "Gloria", "Christian", "Teresa", "Austin", "Ann",
	"Jesse", "Sara", "Dylan", "Madison", "Billy", "Frances", "Bruce", "Kathryn",
	"Ralph", "Janice", "Roy", "Jean", "Eugene", "Abigail", "Randy", "Alice",
	"Wayne", "Judy", "Vincent", "Sophia", "Philip", "Grace", "Bobby", "Denise",
	"Russell", "Amber", "Howard", "Doris", "Louis", "Marilyn", "Harry", "Danielle",
	"Fred", "Beverly", "Martin", "Isabella", "Alan", "Theresa", "Joe", "Diana",
	"Craig", "Natalie", "Walter", "Brittany", "Terry", "Charlotte", "Danny", "Marie",
	"Ernest", "Kayla", "Phillip", "Alexis", "Todd", "Lori",
}

var lastNames = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
	"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
	"Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
	"White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
	"Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
	"Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
	"Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
	"Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales",
	"Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson",
	"Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward",
	"Richardson", "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray",
	"Mendoza", "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel",
	"Myers", "Long", "Ross", "Foster", "Jimenez", "Powell",
}

var loremWords = []string{
	"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
	"sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore",
	"magna", "aliqua", "enim", "ad", "minim", "veniam", "quis", "nostrud",
	"exercitation", "ullamco", "laboris", "nisi", "aliquip", "ex", "ea", "commodo",
	"consequat", "duis", "aute", "irure", "in", "reprehenderit", "voluptate",
	"velit", "esse", "cillum", "fugiat", "nulla", "pariatur", "excepteur", "sint",
	"occaecat", "cupidatat", "non", "proident", "sunt", "culpa", "qui", "officia",
	"deserunt", "mollit", "anim", "id", "est", "the", "quick", "brown", "fox",
	"jumps", "over", "lazy", "dog", "product", "service", "platform", "digital",
	"cloud", "data", "system", "network", "security", "performance", "solution",
	"integration", "analytics", "automation", "infrastructure", "management",
	"enterprise", "scalable", "reliable", "efficient", "innovative", "modern",
	"advanced", "premium", "professional", "dynamic", "global", "strategic",
	"customer", "market", "growth", "development", "technology", "software",
}

var emailDomains = []string{
	"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "protonmail.com",
	"icloud.com", "mail.com", "fastmail.com", "zoho.com", "aol.com",
	"example.com", "test.com", "company.org", "corp.net", "business.io",
}

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 Safari/17.0",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) AppleWebKit/605.1.15 Mobile/15E148",
	"Mozilla/5.0 (iPad; CPU OS 17_0) AppleWebKit/605.1.15 Mobile/15E148",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
	"Mozilla/5.0 (Android 14; Mobile) AppleWebKit/537.36 Chrome/120.0 Mobile",
}

var actionVerbs = []string{
	"create", "update", "delete", "login", "logout", "view", "export", "import",
	"approve", "reject", "archive", "restore", "publish", "unpublish",
}

var entityTypes = []string{
	"User", "Product", "Order", "Post", "Comment", "Category", "Payment",
	"Shipment", "Review", "Notification", "Address", "Invoice",
}

var notificationTypes = []string{
	"order.placed", "order.shipped", "order.delivered", "payment.received",
	"review.posted", "comment.reply", "account.welcome", "password.reset",
	"product.back_in_stock", "promotion.discount",
}

var carriers = []string{
	"UPS", "FedEx", "USPS", "DHL", "Amazon Logistics", "Royal Mail", "Canada Post",
}

var paymentMethods = []string{
	"credit_card", "debit_card", "paypal", "bank_transfer", "apple_pay", "google_pay", "crypto",
}

var postStatuses = []string{"draft", "published", "archived"}

var orderStatuses = []string{"pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"}

var paymentStatuses = []string{"pending", "completed", "failed", "refunded"}

var shipmentStatuses = []string{"processing", "shipped", "in_transit", "out_for_delivery", "delivered", "returned"}

var countryCodes = []string{
	"US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "MX",
	"IT", "ES", "NL", "SE", "NO", "DK", "FI", "PL", "CZ", "AT",
	"CH", "BE", "PT", "IE", "NZ", "SG", "KR", "TW", "HK", "ZA",
}

var currencyCodes = []string{
	"USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "CNY", "SEK", "NZD",
}

// ── ID tracking ──

// IDPool tracks generated IDs per table for FK references.
type IDPool struct {
	mu   sync.RWMutex
	pool map[string][]int
}

func newIDPool() *IDPool {
	return &IDPool{pool: make(map[string][]int)}
}

func (p *IDPool) addIDs(table string, ids []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pool[table] = append(p.pool[table], ids...)
}

func (p *IDPool) randomID(table string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ids := p.pool[table]
	if len(ids) == 0 {
		return 1
	}
	return ids[fastRandN(len(ids))]
}

func (p *IDPool) count(table string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.pool[table])
}

// ── Fast random helpers (non-crypto, fine for test data) ──

var rngMu sync.Mutex
var rng = mrand.New(mrand.NewSource(time.Now().UnixNano()))

func fastRandN(n int) int {
	if n <= 0 {
		return 0
	}
	rngMu.Lock()
	v := rng.Intn(n)
	rngMu.Unlock()
	return v
}

func fastRandFloat(min, max float64) float64 {
	rngMu.Lock()
	v := min + rng.Float64()*(max-min)
	rngMu.Unlock()
	return v
}

func fastRandBool() bool {
	return fastRandN(2) == 0
}

// ── Generator helpers ──

func randomFrom(pool []string) string {
	return pool[fastRandN(len(pool))]
}

func randomFirstName() string { return randomFrom(firstNames) }
func randomLastName() string  { return randomFrom(lastNames) }

func randomEmail(rowID int) string {
	return fmt.Sprintf("%s.%s_%d@%s",
		strings.ToLower(randomFirstName()),
		strings.ToLower(randomLastName()),
		rowID,
		randomFrom(emailDomains),
	)
}

func randomUsername(rowID int) string {
	return fmt.Sprintf("%s_%s_%d",
		strings.ToLower(randomFirstName()),
		strings.ToLower(randomLastName()),
		rowID,
	)
}

func randomSlug(base string, rowID int) string {
	words := make([]string, 2+fastRandN(3))
	for i := range words {
		words[i] = randomFrom(loremWords)
	}
	return fmt.Sprintf("%s-%s-%d", base, strings.Join(words, "-"), rowID)
}

func randomSentence(wordCount int) string {
	words := make([]string, wordCount)
	for i := range words {
		words[i] = randomFrom(loremWords)
	}
	s := strings.Join(words, " ")
	if len(s) > 0 {
		s = strings.ToUpper(s[:1]) + s[1:]
	}
	return s + "."
}

func randomParagraph() string {
	sentences := 3 + fastRandN(5)
	parts := make([]string, sentences)
	for i := range parts {
		parts[i] = randomSentence(5 + fastRandN(10))
	}
	return strings.Join(parts, " ")
}

func randomIP() string {
	return fmt.Sprintf("%d.%d.%d.%d", 1+fastRandN(254), fastRandN(256), fastRandN(256), 1+fastRandN(254))
}

func randomPrice(min, max float64) float64 {
	return float64(int(fastRandFloat(min, max)*100)) / 100
}

func randomSKU(rowID int) string {
	return fmt.Sprintf("SKU-%06d", rowID)
}

func randomOrderNumber(rowID int) string {
	return fmt.Sprintf("ORD-%08d", rowID)
}

func randomTrackingNumber() string {
	n, _ := crand.Int(crand.Reader, big.NewInt(999999999999))
	return fmt.Sprintf("1Z%012d", n.Int64())
}

func randomTransactionID() string {
	n, _ := crand.Int(crand.Reader, big.NewInt(999999999999999))
	return fmt.Sprintf("txn_%015d", n.Int64())
}

func randomTimestamp(daysBack int) time.Time {
	offset := time.Duration(fastRandN(daysBack*24)) * time.Hour
	return time.Now().Add(-offset)
}

func randomPasswordHash() string {
	return "$2y$10$" + fmt.Sprintf("%022d", fastRandN(999999999))
}

func randomDate(daysBack int) time.Time {
	offset := time.Duration(fastRandN(daysBack*24)) * time.Hour
	t := time.Now().Add(-offset)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// ── Row generation per table ──

// generateRows builds rows for the given table template.
// startID is the first ID to use, count is number of rows.
// Returns column names and row data for pgx.CopyFrom.
func generateRows(tmpl TableTemplate, startID, count int, idPool *IDPool) ([]string, [][]interface{}) {
	gen, ok := tableGenerators[tmpl.Name]
	if !ok {
		gen = genericRowGenerator
	}

	// Determine columns (skip "id" — we provide it explicitly)
	var colNames []string
	colNames = append(colNames, "id")
	for _, c := range tmpl.Columns {
		if c.Name == "id" {
			continue
		}
		colNames = append(colNames, c.Name)
	}

	rows := make([][]interface{}, count)
	for i := 0; i < count; i++ {
		rowID := startID + i
		rows[i] = gen(tmpl, rowID, idPool)
	}

	return colNames, rows
}

type rowGeneratorFunc func(tmpl TableTemplate, rowID int, idPool *IDPool) []interface{}

var tableGenerators = map[string]rowGeneratorFunc{
	"users":          genUsers,
	"categories":     genCategories,
	"tags":           genTags,
	"countries":      genCountries,
	"currencies":     genCurrencies,
	"roles":          genRoles,
	"warehouses":     genWarehouses,
	"user_profiles":  genUserProfiles,
	"user_roles":     genUserRoles,
	"addresses":      genAddresses,
	"products":       genProducts,
	"posts":          genPosts,
	"sessions":       genSessions,
	"audit_logs":     genAuditLogs,
	"notifications":  genNotifications,
	"orders":         genOrders,
	"product_tags":   genProductTags,
	"product_images": genProductImages,
	"inventory":      genInventory,
	"comments":       genComments,
	"post_tags":      genPostTags,
	"order_items":    genOrderItems,
	"payments":       genPayments,
	"reviews":        genReviews,
	"shipments":      genShipments,
}

func genUsers(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	ts := randomTimestamp(365)
	return []interface{}{
		rowID,
		randomEmail(rowID),
		randomUsername(rowID),
		randomPasswordHash(),
		randomFirstName(),
		randomLastName(),
		fastRandBool(),
		ts,
		ts,
	}
}

func genCategories(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	return []interface{}{
		rowID,
		randomSentence(2),
		randomSlug("cat", rowID),
		randomParagraph(),
		nil, // parent_id — set via post-insert UPDATE
		fastRandN(100),
		randomTimestamp(365),
	}
}

func genTags(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	return []interface{}{
		rowID,
		randomFrom(loremWords) + "_" + fmt.Sprintf("%d", rowID),
		randomSlug("tag", rowID),
		randomTimestamp(365),
	}
}

func genCountries(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	code := "XX"
	if rowID <= len(countryCodes) {
		code = countryCodes[rowID-1]
	} else {
		// CHAR(2) column — generate two uppercase letters from rowID
		a := 'A' + rune((rowID-1)/26%26)
		b := 'A' + rune((rowID-1)%26)
		code = string([]rune{a, b})
	}
	return []interface{}{
		rowID,
		randomSentence(1),
		code,
		fmt.Sprintf("+%d", 1+fastRandN(999)),
	}
}

func genCurrencies(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	code := "XXX"
	if rowID <= len(currencyCodes) {
		code = currencyCodes[rowID-1]
	} else {
		// CHAR(3) column — generate three uppercase letters from rowID
		a := 'A' + rune((rowID-1)/676%26)
		b := 'A' + rune((rowID-1)/26%26)
		c := 'A' + rune((rowID-1)%26)
		code = string([]rune{a, b, c})
	}
	return []interface{}{
		rowID,
		code,
		randomSentence(1),
		"$",
		int16(2),
	}
}

func genRoles(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	return []interface{}{
		rowID,
		fmt.Sprintf("role_%d", rowID),
		randomSentence(5),
		randomTimestamp(365),
	}
}

func genWarehouses(_ TableTemplate, rowID int, _ *IDPool) []interface{} {
	return []interface{}{
		rowID,
		fmt.Sprintf("Warehouse %d", rowID),
		fmt.Sprintf("WH-%04d", rowID),
		randomSentence(1),
		true,
		randomTimestamp(365),
	}
}

func genUserProfiles(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	return []interface{}{
		rowID,
		idPool.randomID("users"),
		randomParagraph(),
		fmt.Sprintf("https://cdn.example.com/avatars/%d.jpg", rowID),
		randomDate(365 * 30),
		fmt.Sprintf("+1-%03d-%03d-%04d", fastRandN(999), fastRandN(999), fastRandN(9999)),
		randomTimestamp(90),
	}
}

func genUserRoles(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	return []interface{}{
		rowID,
		idPool.randomID("users"),
		idPool.randomID("roles"),
		randomTimestamp(365),
	}
}

func genAddresses(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	return []interface{}{
		rowID,
		idPool.randomID("users"),
		idPool.randomID("countries"),
		fmt.Sprintf("%d %s St", 1+fastRandN(9999), randomLastName()),
		nil,
		randomSentence(1),
		randomSentence(1),
		fmt.Sprintf("%05d", fastRandN(99999)),
		rowID == 1,
		randomTimestamp(365),
	}
}

func genProducts(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	ts := randomTimestamp(365)
	return []interface{}{
		rowID,
		idPool.randomID("categories"),
		idPool.randomID("currencies"),
		randomSentence(3),
		randomSlug("prod", rowID),
		randomParagraph(),
		randomPrice(1, 999),
		randomPrice(0.5, 500),
		randomSKU(rowID),
		fastRandBool(),
		ts,
		ts,
	}
}

func genPosts(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	ts := randomTimestamp(365)
	status := randomFrom(postStatuses)
	var publishedAt interface{}
	if status == "published" {
		publishedAt = ts
	}
	return []interface{}{
		rowID,
		idPool.randomID("users"),
		randomSentence(5),
		randomSlug("post", rowID),
		randomParagraph() + "\n\n" + randomParagraph(),
		status,
		publishedAt,
		ts,
		ts,
	}
}

func genSessions(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	ts := randomTimestamp(30)
	return []interface{}{
		rowID,
		idPool.randomID("users"),
		fmt.Sprintf("sess_%d_%d", rowID, fastRandN(999999)),
		randomIP(),
		randomFrom(userAgents),
		ts,
		ts,
	}
}

func genAuditLogs(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	var userID interface{}
	if fastRandN(10) > 1 {
		userID = idPool.randomID("users")
	}
	return []interface{}{
		rowID,
		userID,
		randomFrom(actionVerbs),
		randomFrom(entityTypes),
		1 + fastRandN(10000),
		nil,
		nil,
		randomIP(),
		randomTimestamp(90),
	}
}

func genNotifications(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	isRead := fastRandBool()
	var readAt interface{}
	if isRead {
		readAt = randomTimestamp(30)
	}
	return []interface{}{
		rowID,
		idPool.randomID("users"),
		randomFrom(notificationTypes),
		randomSentence(4),
		randomSentence(8),
		isRead,
		readAt,
		randomTimestamp(60),
	}
}

func genOrders(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	subtotal := randomPrice(10, 5000)
	tax := float64(int(subtotal*0.08*100)) / 100
	total := float64(int((subtotal+tax)*100)) / 100
	ts := randomTimestamp(365)

	var addressID interface{}
	if idPool.count("addresses") > 0 {
		addressID = idPool.randomID("addresses")
	}

	return []interface{}{
		rowID,
		idPool.randomID("users"),
		idPool.randomID("currencies"),
		addressID,
		randomOrderNumber(rowID),
		randomFrom(orderStatuses),
		subtotal,
		tax,
		total,
		nil,
		ts,
		ts,
	}
}

func genProductTags(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	return []interface{}{
		rowID,
		idPool.randomID("products"),
		idPool.randomID("tags"),
		randomTimestamp(365),
	}
}

func genProductImages(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	return []interface{}{
		rowID,
		idPool.randomID("products"),
		fmt.Sprintf("https://cdn.example.com/products/%d_%d.jpg", idPool.randomID("products"), rowID),
		randomSentence(3),
		fastRandN(10),
		randomTimestamp(365),
	}
}

func genInventory(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	qty := fastRandN(1000)
	reserved := fastRandN(qty + 1)
	return []interface{}{
		rowID,
		idPool.randomID("products"),
		idPool.randomID("warehouses"),
		qty,
		reserved,
		10 + fastRandN(90),
		randomTimestamp(30),
	}
}

func genComments(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	ts := randomTimestamp(180)
	return []interface{}{
		rowID,
		idPool.randomID("posts"),
		idPool.randomID("users"),
		nil, // parent_id — set via post-insert UPDATE
		randomParagraph(),
		fastRandN(10) > 1,
		ts,
		ts,
	}
}

func genPostTags(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	return []interface{}{
		rowID,
		idPool.randomID("posts"),
		idPool.randomID("tags"),
		randomTimestamp(365),
	}
}

func genOrderItems(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	qty := 1 + fastRandN(5)
	unitPrice := randomPrice(5, 500)
	totalPrice := float64(int(float64(qty)*unitPrice*100)) / 100
	return []interface{}{
		rowID,
		idPool.randomID("orders"),
		idPool.randomID("products"),
		qty,
		unitPrice,
		totalPrice,
		randomTimestamp(365),
	}
}

func genPayments(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	status := randomFrom(paymentStatuses)
	var paidAt interface{}
	if status == "completed" {
		paidAt = randomTimestamp(30)
	}
	return []interface{}{
		rowID,
		idPool.randomID("orders"),
		randomPrice(10, 5000),
		idPool.randomID("currencies"),
		randomFrom(paymentMethods),
		status,
		randomTransactionID(),
		paidAt,
		randomTimestamp(365),
	}
}

func genReviews(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	var orderID interface{}
	if idPool.count("orders") > 0 && fastRandBool() {
		orderID = idPool.randomID("orders")
	}
	return []interface{}{
		rowID,
		idPool.randomID("products"),
		idPool.randomID("users"),
		orderID,
		int16(1 + fastRandN(5)),
		randomSentence(4),
		randomParagraph(),
		fastRandBool(),
		randomTimestamp(180),
	}
}

func genShipments(_ TableTemplate, rowID int, idPool *IDPool) []interface{} {
	status := randomFrom(shipmentStatuses)
	var shippedAt, deliveredAt interface{}
	if status != "processing" {
		shippedAt = randomTimestamp(30)
	}
	if status == "delivered" {
		deliveredAt = randomTimestamp(7)
	}

	var addressID interface{}
	if idPool.count("addresses") > 0 {
		addressID = idPool.randomID("addresses")
	}

	return []interface{}{
		rowID,
		idPool.randomID("orders"),
		addressID,
		randomTrackingNumber(),
		randomFrom(carriers),
		status,
		shippedAt,
		deliveredAt,
		randomTimestamp(60),
	}
}

// genericRowGenerator is a fallback for tables without a specific generator.
func genericRowGenerator(tmpl TableTemplate, rowID int, idPool *IDPool) []interface{} {
	row := []interface{}{rowID}

	for _, c := range tmpl.Columns {
		if c.Name == "id" {
			continue
		}

		// Check if it's a FK column
		var fkTable string
		for _, fk := range tmpl.ForeignKeys {
			if fk.Column == c.Name {
				fkTable = fk.RefTable
				break
			}
		}

		if fkTable != "" {
			if c.Nullable && fastRandN(10) < 2 {
				row = append(row, nil)
			} else {
				row = append(row, idPool.randomID(fkTable))
			}
			continue
		}

		// Generate by type
		upperType := strings.ToUpper(c.Type)
		switch {
		case strings.Contains(upperType, "SERIAL") || strings.Contains(upperType, "PRIMARY KEY"):
			continue
		case strings.Contains(upperType, "BOOLEAN"):
			row = append(row, fastRandBool())
		case strings.Contains(upperType, "SMALLINT"):
			row = append(row, int16(fastRandN(100)))
		case strings.Contains(upperType, "INTEGER"):
			if c.Nullable && fastRandN(10) < 2 {
				row = append(row, nil)
			} else {
				row = append(row, 1+fastRandN(10000))
			}
		case strings.Contains(upperType, "NUMERIC"):
			row = append(row, randomPrice(1, 9999))
		case strings.Contains(upperType, "DOUBLE") || strings.Contains(upperType, "FLOAT"):
			row = append(row, fastRandFloat(0, 100000))
		case strings.Contains(upperType, "TEXT"):
			if c.Nullable && fastRandN(10) < 2 {
				row = append(row, nil)
			} else {
				row = append(row, randomParagraph())
			}
		case strings.Contains(upperType, "VARCHAR"):
			if c.Unique {
				row = append(row, fmt.Sprintf("%s_%d", randomFrom(loremWords), rowID))
			} else if c.Nullable && fastRandN(10) < 2 {
				row = append(row, nil)
			} else {
				row = append(row, randomSentence(3))
			}
		case strings.Contains(upperType, "CHAR"):
			row = append(row, fmt.Sprintf("%02d", rowID%100))
		case strings.Contains(upperType, "TIMESTAMP"):
			if c.Nullable && fastRandN(10) < 3 {
				row = append(row, nil)
			} else {
				row = append(row, randomTimestamp(365))
			}
		case strings.Contains(upperType, "DATE"):
			if c.Nullable && fastRandN(10) < 3 {
				row = append(row, nil)
			} else {
				row = append(row, randomDate(365))
			}
		default:
			row = append(row, fmt.Sprintf("val_%d", rowID))
		}
	}

	return row
}

// ── Row count distribution ──

// computeRowCounts distributes rows across tables based on RowWeight to reach targetBytes.
// Each row is estimated at ~200 bytes on average.
func computeRowCounts(tables []TableTemplate, targetBytes int64) map[string]int {
	const avgRowBytes = 200

	totalWeight := 0
	for _, t := range tables {
		totalWeight += t.RowWeight
	}
	if totalWeight == 0 {
		totalWeight = 1
	}

	totalRows := int(targetBytes / avgRowBytes)
	if totalRows < len(tables) {
		totalRows = len(tables)
	}

	counts := make(map[string]int, len(tables))
	for _, t := range tables {
		rows := (totalRows * t.RowWeight) / totalWeight
		if rows < 1 {
			rows = 1
		}
		counts[t.Name] = rows
	}

	return counts
}

// ── Populate tables ──

// populateTables inserts data into all tables in topological order.
func populateTables(ctx context.Context, conn *pgx.Conn, tables []TableTemplate, targetBytes int64) error {
	idPool := newIDPool()
	rowCounts := computeRowCounts(tables, targetBytes)

	for _, tmpl := range tables {
		count := rowCounts[tmpl.Name]
		if count == 0 {
			continue
		}

		const batchSize = 1000
		startID := 1
		for remaining := count; remaining > 0; {
			batch := batchSize
			if batch > remaining {
				batch = remaining
			}

			colNames, rows := generateRows(tmpl, startID, batch, idPool)

			_, err := conn.CopyFrom(ctx, pgx.Identifier{tmpl.Name}, colNames, pgx.CopyFromRows(rows))
			if err != nil {
				return fmt.Errorf("copy into %s (startID=%d, batch=%d): %w", tmpl.Name, startID, batch, err)
			}

			// Register IDs
			ids := make([]int, batch)
			for i := 0; i < batch; i++ {
				ids[i] = startID + i
			}
			idPool.addIDs(tmpl.Name, ids)

			startID += batch
			remaining -= batch
		}

		// Set sequence to max ID
		maxID := startID - 1
		seqName := tmpl.Name + "_id_seq"
		_, err := conn.Exec(ctx, fmt.Sprintf("SELECT setval('%s', %d, true)", seqName, maxID))
		if err != nil {
			// Not all tables will have sequences with this exact name — ignore
		}
	}

	// Post-insert self-referential updates
	for _, sr := range selfRefColumns() {
		if idPool.count(sr.Table) > 0 {
			applySelfRefUpdates(ctx, conn, sr, idPool)
		}
	}

	return nil
}

// applySelfRefUpdates sets ~30% of rows to reference existing IDs from the same table.
func applySelfRefUpdates(ctx context.Context, conn *pgx.Conn, sr SelfRefUpdate, idPool *IDPool) {
	count := idPool.count(sr.Table)
	updateCount := count * 30 / 100
	if updateCount == 0 {
		return
	}

	// Batch update using a CTE for efficiency
	query := fmt.Sprintf(
		`UPDATE %s SET %s = sub.parent FROM (
			SELECT id, (random() * %d + 1)::int AS parent
			FROM %s
			WHERE random() < 0.3
		) sub
		WHERE %s.id = sub.id AND sub.parent <> %s.id`,
		pgIdentifier(sr.Table),
		pgIdentifier(sr.Column),
		count,
		pgIdentifier(sr.Table),
		pgIdentifier(sr.Table),
		pgIdentifier(sr.Table),
	)
	_, _ = conn.Exec(ctx, query)
}
