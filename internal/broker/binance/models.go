package binance

// RawBookTicker represents the JSON payload from Binance bookTicker endpoint.
type RawBookTicker struct {
	Symbol   string `json:"symbol"`
	BidPrice string `json:"bidPrice"`
	BidQty   string `json:"bidQty"`
	AskPrice string `json:"askPrice"`
	AskQty   string `json:"askQty"`
}

// ExchangeInfoResponse represents the JSON payload from Binance exchangeInfo endpoint.
type ExchangeInfoResponse struct {
	Symbols []ExchangeInfoSymbol `json:"symbols"`
}

// ExchangeInfoSymbol represents an individual symbol definition in exchangeInfo.
type ExchangeInfoSymbol struct {
	Symbol     string `json:"symbol"`
	Status     string `json:"status"`
	BaseAsset  string `json:"baseAsset"`
	QuoteAsset string `json:"quoteAsset"`
}

// OrderResponse represents the response from Binance POST /api/v3/order.
type OrderResponse struct {
	Symbol              string `json:"symbol"`
	OrderID             int64  `json:"orderId"`
	ClientOrderID       string `json:"clientOrderId"`
	TransactTime        int64  `json:"transactTime"`
	Price               string `json:"price"`
	OrigQty             string `json:"origQty"`
	ExecutedQty         string `json:"executedQty"`
	CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
	Status              string `json:"status"`
	TimeInForce         string `json:"timeInForce"`
	Type                string `json:"type"`
	Side                string `json:"side"`
	Code                int    `json:"code,omitempty"`
	Msg                 string `json:"msg,omitempty"`
}

// AccountResponse represents the response from Binance GET /api/v3/account.
type AccountResponse struct {
	MakerCommission  int64            `json:"makerCommission"`
	TakerCommission  int64            `json:"takerCommission"`
	BuyerCommission  int64            `json:"buyerCommission"`
	SellerCommission int64            `json:"sellerCommission"`
	CanTrade         bool             `json:"canTrade"`
	CanWithdraw      bool             `json:"canWithdraw"`
	CanDeposit       bool             `json:"canDeposit"`
	UpdateTime       int64            `json:"updateTime"`
	AccountType      string           `json:"accountType"`
	Balances         []AccountBalance `json:"balances"`
	Code             int              `json:"code,omitempty"`
	Msg              string           `json:"msg,omitempty"`
}

// AccountBalance represents an individual asset balance on Binance.
type AccountBalance struct {
	Asset  string `json:"asset"`
	Free   string `json:"free"`
	Locked string `json:"locked"`
}

// APIError represents standard Binance error structure.
type APIError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}
