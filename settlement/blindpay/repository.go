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
	TenantID, ProviderCustomerID, KYCStatus string
}

func (r CustomerReference) Validate() error {
	if strings.TrimSpace(r.TenantID) == "" || !strings.HasPrefix(r.ProviderCustomerID, "re_") {
		return errors.New("tenant ID and BlindPay customer ID are required")
	}
	switch r.KYCStatus {
	case "verifying", "approved", "rejected", "compliance_request", "approved_rfi":
		return nil
	default:
		return errors.New("invalid BlindPay customer KYC status")
	}
}

type BankAccountReference struct {
	ProviderBankAccountID, TenantID            string
	Rail, DisplayName, AccountLastFour, Status string
}

func (r BankAccountReference) Validate() error {
	if !strings.HasPrefix(r.ProviderBankAccountID, "ba_") || r.TenantID == "" || r.Rail == "" || r.DisplayName == "" || len(r.AccountLastFour) > 4 {
		return errors.New("invalid BlindPay bank account reference")
	}
	switch r.Status {
	case "pending", "approved", "rejected":
		return nil
	}
	return errors.New("invalid BlindPay bank account status")
}

type ManagedWalletReference struct {
	ProviderWalletID, TenantID            string
	Network, Address, DisplayName, Status string
}

func (r ManagedWalletReference) Validate() error {
	if !strings.HasPrefix(r.ProviderWalletID, "bl_") || r.TenantID == "" || r.Network == "" || r.Address == "" || r.DisplayName == "" {
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
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_customers(tenant_id,provider_customer_id,kyc_status,created_at,updated_at) VALUES($1,$2,$3,$4,$4) ON CONFLICT(tenant_id) DO UPDATE SET provider_customer_id=EXCLUDED.provider_customer_id,kyc_status=EXCLUDED.kyc_status,updated_at=EXCLUDED.updated_at`, ref.TenantID, ref.ProviderCustomerID, ref.KYCStatus, now)
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
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_bank_accounts(provider_bank_account_id,tenant_id,rail,display_name,account_last_four,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$7) ON CONFLICT(provider_bank_account_id) DO UPDATE SET rail=EXCLUDED.rail,display_name=EXCLUDED.display_name,account_last_four=EXCLUDED.account_last_four,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at WHERE blindpay_bank_accounts.tenant_id=EXCLUDED.tenant_id`, ref.ProviderBankAccountID, ref.TenantID, ref.Rail, ref.DisplayName, ref.AccountLastFour, ref.Status, now)
	if err != nil {
		return fmt.Errorf("upsert BlindPay bank account reference: %w", err)
	}
	return r.upsertProviderResource(ctx, ref.ProviderBankAccountID, ref.TenantID, "payment_instrument", ref.ProviderBankAccountID, map[string]any{"kind": "bank_account", "rail": ref.Rail})
}

func (r *Repository) UpsertManagedWallet(ctx context.Context, ref ManagedWalletReference) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	now := r.now()
	_, err := r.db.ExecContext(ctx, `INSERT INTO blindpay_managed_wallets(provider_wallet_id,tenant_id,network,address,display_name,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$7) ON CONFLICT(provider_wallet_id) DO UPDATE SET network=EXCLUDED.network,address=EXCLUDED.address,display_name=EXCLUDED.display_name,status=EXCLUDED.status,updated_at=EXCLUDED.updated_at WHERE blindpay_managed_wallets.tenant_id=EXCLUDED.tenant_id`, ref.ProviderWalletID, ref.TenantID, ref.Network, ref.Address, ref.DisplayName, ref.Status, now)
	if err != nil {
		return fmt.Errorf("upsert BlindPay managed wallet reference: %w", err)
	}
	return r.upsertProviderResource(ctx, ref.ProviderWalletID, ref.TenantID, "account", ref.ProviderWalletID, map[string]any{"kind": "managed_wallet", "network": ref.Network, "address": ref.Address})
}

type ProviderResource struct {
	ProviderReference string
	Metadata          json.RawMessage
}

func (r *Repository) LoadPayoutExecutionContext(ctx context.Context, providerQuoteID string) (json.RawMessage, error) {
	var executionContext json.RawMessage
	err := r.db.QueryRowContext(ctx, `SELECT provider_execution_context FROM payment_quotes WHERE direction='payout' AND provider='blindpay' AND provider_quote_id=$1`, providerQuoteID).Scan(&executionContext)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrReferenceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load BlindPay payout execution context: %w", err)
	}
	return executionContext, nil
}

func (r *Repository) ResolveProviderResource(ctx context.Context, tenantID, id, resourceType string) (ProviderResource, error) {
	var resource ProviderResource
	err := r.db.QueryRowContext(ctx, `SELECT provider_reference,metadata FROM provider_resources WHERE id=$1 AND tenant_id=$2 AND provider='blindpay' AND resource_type=$3`, id, tenantID, resourceType).Scan(&resource.ProviderReference, &resource.Metadata)
	if errors.Is(err, sql.ErrNoRows) {
		return ProviderResource{}, ErrReferenceNotFound
	}
	if err != nil {
		return ProviderResource{}, fmt.Errorf("resolve BlindPay provider resource: %w", err)
	}
	return resource, nil
}

func (r *Repository) upsertProviderResource(ctx context.Context, id, tenantID, resourceType, providerReference string, metadata map[string]any) error {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	now := r.now()
	_, err = r.db.ExecContext(ctx, `INSERT INTO provider_resources(id,tenant_id,provider,resource_type,provider_reference,metadata,created_at,updated_at) VALUES($1,$2,'blindpay',$3,$4,$5,$6,$6) ON CONFLICT(id) DO UPDATE SET provider_reference=EXCLUDED.provider_reference,metadata=EXCLUDED.metadata,updated_at=EXCLUDED.updated_at WHERE provider_resources.tenant_id=EXCLUDED.tenant_id AND provider_resources.provider='blindpay'`, id, tenantID, resourceType, providerReference, raw, now)
	if err != nil {
		return fmt.Errorf("upsert BlindPay provider resource: %w", err)
	}
	return nil
}

func (r *Repository) GetApprovedPayoutProfile(ctx context.Context, tenantID, bankAccountID, walletID string) (PayoutProfile, error) {
	var p PayoutProfile
	err := r.db.QueryRowContext(ctx, `SELECT c.tenant_id,c.provider_customer_id,c.kyc_status,b.provider_bank_account_id,b.rail,b.display_name,b.account_last_four,b.status,w.provider_wallet_id,w.network,w.address,w.display_name,w.status FROM blindpay_customers c JOIN blindpay_bank_accounts b ON b.tenant_id=c.tenant_id JOIN blindpay_managed_wallets w ON w.tenant_id=c.tenant_id WHERE c.tenant_id=$1 AND b.provider_bank_account_id=$2 AND w.provider_wallet_id=$3`, tenantID, bankAccountID, walletID).Scan(&p.Customer.TenantID, &p.Customer.ProviderCustomerID, &p.Customer.KYCStatus, &p.BankAccount.ProviderBankAccountID, &p.BankAccount.Rail, &p.BankAccount.DisplayName, &p.BankAccount.AccountLastFour, &p.BankAccount.Status, &p.ManagedWallet.ProviderWalletID, &p.ManagedWallet.Network, &p.ManagedWallet.Address, &p.ManagedWallet.DisplayName, &p.ManagedWallet.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return PayoutProfile{}, ErrReferenceNotFound
	}
	if err != nil {
		return PayoutProfile{}, fmt.Errorf("get BlindPay payout profile: %w", err)
	}
	p.BankAccount.TenantID, p.ManagedWallet.TenantID = tenantID, tenantID
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
