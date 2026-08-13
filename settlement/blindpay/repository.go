package blindpay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrReferenceNotFound      = errors.New("BlindPay reference not found")
	ErrCustomerNotApproved    = errors.New("BlindPay customer is not approved")
	ErrBankAccountNotApproved = errors.New("BlindPay bank account is not approved")
	ErrManagedWalletInactive  = errors.New("BlindPay managed wallet is inactive")
)

type CustomerReference struct {
	LocalCustomerID, ProviderCustomerID, KYCStatus string
}

func (r CustomerReference) Validate() error {
	if strings.TrimSpace(r.LocalCustomerID) == "" || !strings.HasPrefix(r.ProviderCustomerID, "re_") {
		return errors.New("local customer ID and BlindPay customer ID are required")
	}
	switch r.KYCStatus {
	case "verifying", "approved", "rejected", "compliance_request", "approved_rfi":
		return nil
	default:
		return errors.New("invalid BlindPay customer KYC status")
	}
}

type BankAccountReference struct {
	ProviderBankAccountID, LocalCustomerID     string
	Rail, DisplayName, AccountLastFour, Status string
}

func (r BankAccountReference) Validate() error {
	if !strings.HasPrefix(r.ProviderBankAccountID, "ba_") || r.LocalCustomerID == "" || r.Rail == "" || r.DisplayName == "" || len(r.AccountLastFour) > 4 {
		return errors.New("invalid BlindPay bank account reference")
	}
	switch r.Status {
	case "pending", "approved", "rejected":
		return nil
	}
	return errors.New("invalid BlindPay bank account status")
}

type ManagedWalletReference struct {
	ProviderWalletID, LocalCustomerID     string
	Network, Address, DisplayName, Status string
}

func (r ManagedWalletReference) Validate() error {
	if !strings.HasPrefix(r.ProviderWalletID, "bl_") || r.LocalCustomerID == "" || r.Network == "" || r.Address == "" || r.DisplayName == "" {
		return errors.New("invalid BlindPay managed wallet reference")
	}
	if r.Status != "active" && r.Status != "disabled" {
		return errors.New("invalid BlindPay managed wallet status")
	}
	return nil
}

type PayoutProfile struct {
	Customer      CustomerReference
	BankAccount   BankAccountReference
	ManagedWallet ManagedWalletReference
}

type Repository struct {
	db  *sql.DB
	now func() time.Time
}

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("BlindPay reference database is required")
	}
	return &Repository{db: db, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (r *Repository) UpsertCustomer(ctx context.Context, ref CustomerReference) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	now := r.now()
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_customers(local_customer_id,provider_customer_id,kyc_status,created_at,updated_at) VALUES($1,$2,$3,$4,$4) ON CONFLICT(local_customer_id) DO UPDATE SET provider_customer_id=EXCLUDED.provider_customer_id,kyc_status=EXCLUDED.kyc_status,updated_at=EXCLUDED.updated_at`, ref.LocalCustomerID, ref.ProviderCustomerID, ref.KYCStatus, now)
	if err != nil {
		return fmt.Errorf("upsert BlindPay customer reference: %w", err)
	}
	return nil
}

func (r *Repository) UpsertBankAccount(ctx context.Context, ref BankAccountReference) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	now := r.now()
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_bank_accounts(provider_bank_account_id,local_customer_id,rail,display_name,account_last_four,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$7) ON CONFLICT(provider_bank_account_id) DO UPDATE SET rail=EXCLUDED.rail,display_name=EXCLUDED.display_name,account_last_four=EXCLUDED.account_last_four,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at WHERE blindpay_bank_accounts.local_customer_id=EXCLUDED.local_customer_id`, ref.ProviderBankAccountID, ref.LocalCustomerID, ref.Rail, ref.DisplayName, ref.AccountLastFour, ref.Status, now)
	if err != nil {
		return fmt.Errorf("upsert BlindPay bank account reference: %w", err)
	}
	return nil
}

func (r *Repository) UpsertManagedWallet(ctx context.Context, ref ManagedWalletReference) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	now := r.now()
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_managed_wallets(provider_wallet_id,local_customer_id,network,address,display_name,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$7) ON CONFLICT(provider_wallet_id) DO UPDATE SET network=EXCLUDED.network,address=EXCLUDED.address,display_name=EXCLUDED.display_name,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at WHERE blindpay_managed_wallets.local_customer_id=EXCLUDED.local_customer_id`, ref.ProviderWalletID, ref.LocalCustomerID, ref.Network, ref.Address, ref.DisplayName, ref.Status, now)
	if err != nil {
		return fmt.Errorf("upsert BlindPay managed wallet reference: %w", err)
	}
	return nil
}

func (r *Repository) GetApprovedPayoutProfile(ctx context.Context, localCustomerID, bankAccountID, walletID string) (PayoutProfile, error) {
	var p PayoutProfile
	err := r.db.QueryRowContext(ctx, `SELECT c.local_customer_id,c.provider_customer_id,c.kyc_status,b.provider_bank_account_id,b.rail,b.display_name,b.account_last_four,b.status,w.provider_wallet_id,w.network,w.address,w.display_name,w.status FROM blindpay_customers c JOIN blindpay_bank_accounts b ON b.local_customer_id=c.local_customer_id JOIN blindpay_managed_wallets w ON w.local_customer_id=c.local_customer_id WHERE c.local_customer_id=$1 AND b.provider_bank_account_id=$2 AND w.provider_wallet_id=$3`, localCustomerID, bankAccountID, walletID).Scan(&p.Customer.LocalCustomerID, &p.Customer.ProviderCustomerID, &p.Customer.KYCStatus, &p.BankAccount.ProviderBankAccountID, &p.BankAccount.Rail, &p.BankAccount.DisplayName, &p.BankAccount.AccountLastFour, &p.BankAccount.Status, &p.ManagedWallet.ProviderWalletID, &p.ManagedWallet.Network, &p.ManagedWallet.Address, &p.ManagedWallet.DisplayName, &p.ManagedWallet.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return PayoutProfile{}, ErrReferenceNotFound
	}
	if err != nil {
		return PayoutProfile{}, fmt.Errorf("get BlindPay payout profile: %w", err)
	}
	p.BankAccount.LocalCustomerID, p.ManagedWallet.LocalCustomerID = localCustomerID, localCustomerID
	if p.Customer.KYCStatus != "approved" && p.Customer.KYCStatus != "approved_rfi" {
		return PayoutProfile{}, ErrCustomerNotApproved
	}
	if p.BankAccount.Status != "approved" {
		return PayoutProfile{}, ErrBankAccountNotApproved
	}
	if p.ManagedWallet.Status != "active" {
		return PayoutProfile{}, ErrManagedWalletInactive
	}
	return p, nil
}

func (r *Repository) SavePayoutQuote(ctx context.Context, q PayoutQuote, raw json.RawMessage) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_quotes(id,provider,provider_quote_id,local_customer_id,provider_bank_account_id,provider_wallet_id,source_currency,destination_currency,currency_type,cover_fees,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,status,expires_at,provider_payload,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$21)`, q.ID, q.Provider, q.ProviderQuoteID, q.LocalCustomerID, q.ProviderBankAccountID, q.ProviderWalletID, q.SourceCurrency, q.DestinationCurrency, q.CurrencyType, q.CoverFees, q.SenderAmountMinor, q.ReceiverAmountMinor, q.CommercialRate, q.ProviderRate, q.FlatFeeMinor, q.PartnerFeeMinor, q.BillingFeeMinor, q.Status, q.ExpiresAt, raw, q.CreatedAt)
	if err != nil {
		return fmt.Errorf("save BlindPay payout quote: %w", err)
	}
	return nil
}
