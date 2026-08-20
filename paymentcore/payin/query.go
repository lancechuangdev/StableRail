package payin

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Service) Get(ctx context.Context, tenantID, id string) (*Payin, error) {
	var payin Payin
	err := s.db.QueryRowContext(ctx, `SELECT id,COALESCE(quote_id,''),provider,COALESCE(provider_payin_id,''),funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&payin.ID, &payin.QuoteID, &payin.Provider, &payin.ProviderPayinID, &payin.FundingMethod, &payin.SourceInstrumentID, &payin.DestinationAccountID, &payin.SourceAmountMinor, &payin.SourceCurrency, &payin.DestinationAmountMinor, &payin.DestinationCurrency, &payin.Status, &payin.Instructions, &payin.CreatedAt, &payin.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &payin, err
}
