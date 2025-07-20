package pricing

import (
	"context"
)

type Price struct {
	PricePerUnit float64
	CurrencyCode string
}

type Store interface {
	GetSkuPrice(ctx context.Context, sku string) Price
}

type pricingStore struct {
}

func NewStore() Store {
	return &pricingStore{}
}

func (p *pricingStore) GetSkuPrice(_ context.Context, _ string) Price {
	// TODO: replace with actual pricing retrieval logic
	// see billing.list_prices table in the databricks db
	return Price{PricePerUnit: 0.22, CurrencyCode: "USD"}
}
