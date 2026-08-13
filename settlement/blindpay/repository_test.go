package blindpay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRepositoryUpsertsSafeReferences(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, _ := NewRepository(db)
	now := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	repo.now = func() time.Time { return now }
	mock.ExpectExec("INSERT INTO blindpay_customers").WithArgs("cus_local", "re_remote", "approved", now).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO blindpay_bank_accounts").WithArgs("ba_remote", "cus_local", "ach", "Payroll", "1234", "approved", now).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO blindpay_managed_wallets").WithArgs("bl_remote", "cus_local", "sepolia", "0xabc", "Settlement", "active", now).WillReturnResult(sqlmock.NewResult(1, 1))
	ctx := context.Background()
	if err := repo.UpsertCustomer(ctx, CustomerReference{"cus_local", "re_remote", "approved"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertBankAccount(ctx, BankAccountReference{"ba_remote", "cus_local", "ach", "Payroll", "1234", "approved"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpsertManagedWallet(ctx, ManagedWalletReference{"bl_remote", "cus_local", "sepolia", "0xabc", "Settlement", "active"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetApprovedPayoutProfile(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	repo, _ := NewRepository(db)
	columns := []string{"local_customer_id", "provider_customer_id", "kyc_status", "provider_bank_account_id", "rail", "display_name", "account_last_four", "status", "provider_wallet_id", "network", "address", "display_name", "status"}
	mock.ExpectQuery("SELECT c.local_customer_id").WithArgs("cus_local", "ba_remote", "bl_remote").WillReturnRows(sqlmock.NewRows(columns).AddRow("cus_local", "re_remote", "approved", "ba_remote", "ach", "Payroll", "1234", "approved", "bl_remote", "sepolia", "0xabc", "Settlement", "active"))
	p, err := repo.GetApprovedPayoutProfile(context.Background(), "cus_local", "ba_remote", "bl_remote")
	if err != nil {
		t.Fatal(err)
	}
	if p.Customer.ProviderCustomerID != "re_remote" || p.ManagedWallet.Address != "0xabc" {
		t.Fatalf("profile=%+v", p)
	}
}

func TestGetPayoutProfileRejectsUnapprovedCustomer(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	repo, _ := NewRepository(db)
	columns := []string{"local_customer_id", "provider_customer_id", "kyc_status", "provider_bank_account_id", "rail", "display_name", "account_last_four", "status", "provider_wallet_id", "network", "address", "display_name", "status"}
	mock.ExpectQuery("SELECT c.local_customer_id").WillReturnRows(sqlmock.NewRows(columns).AddRow("cus_local", "re_remote", "verifying", "ba_remote", "ach", "Payroll", "1234", "approved", "bl_remote", "sepolia", "0xabc", "Settlement", "active"))
	_, err := repo.GetApprovedPayoutProfile(context.Background(), "cus_local", "ba_remote", "bl_remote")
	if !errors.Is(err, ErrCustomerNotApproved) {
		t.Fatalf("error=%v", err)
	}
}

func TestReferenceValidation(t *testing.T) {
	if err := (CustomerReference{LocalCustomerID: "x", ProviderCustomerID: "wrong", KYCStatus: "approved"}).Validate(); err == nil {
		t.Fatal("invalid provider customer ID accepted")
	}
	if err := (BankAccountReference{ProviderBankAccountID: "ba_x", LocalCustomerID: "x", Rail: "ach", DisplayName: "x", AccountLastFour: "12345", Status: "approved"}).Validate(); err == nil {
		t.Fatal("unsafe account suffix accepted")
	}
}
