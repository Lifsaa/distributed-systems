package main

import (
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// dialBank connects to the bank server.
func dialBank(addr string) (*rpc.Client, error) {
	return rpc.Dial("tcp", addr)
}

// callBank dials, calls one RPC, and closes the connection.
func callBank(addr, method string, req any, resp any) error {
	c, err := dialBank(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Call(method, req, resp)
}

// runClient runs one manual op, demo, or load test.
func runClient(
	addr, op, username string,
	account, to, amount int64,
	percent float64,
	clients, requests int,
) error {
	switch op {
	case "open":
		return clientOpen(addr, username, amount)
	case "balance":
		return clientBalance(addr, account)
	case "deposit":
		return clientMoney(addr, "Bank.Deposit", account, amount)
	case "withdraw":
		return clientMoney(addr, "Bank.Withdraw", account, amount)
	case "transfer":
		return clientTransfer(addr, account, to, amount)
	case "close":
		return clientBasic(addr, "Bank.CloseAccount", account)
	case "freeze":
		return clientBasic(addr, "Bank.FreezeAccount", account)
	case "unfreeze":
		return clientBasic(addr, "Bank.UnfreezeAccount", account)
	case "percent":
		return clientPercent(addr, account, percent)
	case "fee":
		return clientMoney(addr, "Bank.ChargeFee", account, amount)
	case "demo":
		return runDemo(addr)
	case "load":
		return runLoad(addr, clients, requests)
	default:
		return fmt.Errorf("unknown client op")
	}
}

// clientOpen calls teller open account.
func clientOpen(addr, username string, balance int64) error {
	var resp AccountResponse
	req := &OpenAccountRequest{
		Username:       username,
		InitialBalance: balance,
	}
	err := callBank(addr, "Bank.OpenAccount", req, &resp)
	fmt.Printf("open: ok=%v err=%q account=%+v\n", resp.OK, resp.Err, resp.Account)
	return err
}

// clientBalance calls customer balance.
func clientBalance(addr string, account int64) error {
	var resp BalanceResponse
	req := &AccountRequest{AccountID: account}
	err := callBank(addr, "Bank.CheckBalance", req, &resp)
	fmt.Printf(
		"balance: ok=%v err=%q balance=%d status=%s\n",
		resp.OK, resp.Err, resp.Balance, resp.Status,
	)
	return err
}

// clientMoney calls an RPC that uses account and amount.
func clientMoney(addr, method string, account, amount int64) error {
	var resp BasicResponse
	err := callBank(addr, method, &MoneyRequest{AccountID: account, Amount: amount}, &resp)
	fmt.Printf("%s: ok=%v err=%q\n", method, resp.OK, resp.Err)
	return err
}

// clientTransfer calls customer transfer.
func clientTransfer(addr string, from, to, amount int64) error {
	var resp BasicResponse
	req := &TransferRequest{
		FromAccountID: from,
		ToAccountID:   to,
		Amount:        amount,
	}
	err := callBank(addr, "Bank.Transfer", req, &resp)
	fmt.Printf("transfer: ok=%v err=%q\n", resp.OK, resp.Err)
	return err
}

// clientBasic calls a teller status RPC.
func clientBasic(addr, method string, account int64) error {
	var resp BasicResponse
	err := callBank(addr, method, &AccountRequest{AccountID: account}, &resp)
	fmt.Printf("%s: ok=%v err=%q\n", method, resp.OK, resp.Err)
	return err
}

// clientPercent calls teller percent update.
func clientPercent(addr string, account int64, percent float64) error {
	var resp BasicResponse
	req := &PercentRequest{AccountID: account, Percent: percent}
	err := callBank(addr, "Bank.ApplyPercent", req, &resp)
	fmt.Printf("percent: ok=%v err=%q\n", resp.OK, resp.Err)
	return err
}

// mustCall prints the step and keeps demo code small.
func mustCall(c *rpc.Client, name string, req any, resp any) {
	err := c.Call(name, req, resp)
	fmt.Printf("%s err=%v resp=%+v\n", name, err, resp)
}

// runDemo runs a small readable banking story.
func runDemo(addr string) error {
	c, err := dialBank(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	var alice, bob AccountResponse
	mustCall(c, "Bank.OpenAccount", &OpenAccountRequest{
		Username:       "alice",
		InitialBalance: 10000,
	}, &alice)
	mustCall(c, "Bank.OpenAccount", &OpenAccountRequest{
		Username:       "bob",
		InitialBalance: 5000,
	}, &bob)
	mustCall(c, "Bank.Deposit", &MoneyRequest{
		AccountID: alice.Account.ID,
		Amount:    2500,
	}, &BasicResponse{})
	mustCall(c, "Bank.Withdraw", &MoneyRequest{
		AccountID: bob.Account.ID,
		Amount:    1000,
	}, &BasicResponse{})
	mustCall(c, "Bank.Transfer", &TransferRequest{
		FromAccountID: alice.Account.ID,
		ToAccountID:   bob.Account.ID,
		Amount:        1500,
	}, &BasicResponse{})
	mustCall(c, "Bank.FreezeAccount", &AccountRequest{
		AccountID: bob.Account.ID,
	}, &BasicResponse{})
	mustCall(c, "Bank.Deposit", &MoneyRequest{
		AccountID: bob.Account.ID,
		Amount:    500,
	}, &BasicResponse{})
	mustCall(c, "Bank.UnfreezeAccount", &AccountRequest{
		AccountID: bob.Account.ID,
	}, &BasicResponse{})
	mustCall(c, "Bank.ApplyPercent", &PercentRequest{
		AccountID: alice.Account.ID,
		Percent:   5,
	}, &BasicResponse{})
	mustCall(c, "Bank.ChargeFee", &MoneyRequest{
		AccountID: alice.Account.ID,
		Amount:    300,
	}, &BasicResponse{})
	mustCall(c, "Bank.CloseAccount", &AccountRequest{
		AccountID: bob.Account.ID,
	}, &BasicResponse{})
	mustCall(c, "Bank.CheckBalance", &AccountRequest{
		AccountID: alice.Account.ID,
	}, &BalanceResponse{})
	return nil
}

// openLoadAccounts creates accounts for the load test.
func openLoadAccounts(addr string, n int) ([]int64, error) {
	c, err := dialBank(addr)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var ids []int64
	for i := 0; i < n; i++ {
		var resp AccountResponse
		err := c.Call("Bank.OpenAccount", &OpenAccountRequest{
			Username:       fmt.Sprintf("load%d", time.Now().UnixNano()+int64(i)),
			InitialBalance: 100000,
		}, &resp)
		if err != nil {
			return nil, err
		}
		if !resp.OK {
			return nil, fmt.Errorf(resp.Err)
		}
		ids = append(ids, resp.Account.ID)
	}
	return ids, nil
}

// runLoad sends many concurrent customer requests.
func runLoad(addr string, clients, requests int) error {
	if clients <= 0 {
		clients = 100
	}
	if requests <= 0 {
		requests = 1000
	}
	ids, err := openLoadAccounts(addr, 10)
	if err != nil {
		return err
	}
	start := time.Now()
	var ok, failed int64
	var sent int64
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			c, err := dialBank(addr)
			if err != nil {
				atomic.AddInt64(&failed, 1)
				return
			}
			defer c.Close()
			for {
				n := atomic.AddInt64(&sent, 1)
				if int(n) > requests {
					return
				}
				id := ids[r.Intn(len(ids))]
				var resp BasicResponse
				if r.Intn(2) == 0 {
					err = c.Call("Bank.Deposit", &MoneyRequest{AccountID: id, Amount: 100}, &resp)
				} else {
					err = c.Call("Bank.Withdraw", &MoneyRequest{AccountID: id, Amount: 50}, &resp)
				}
				if err == nil && resp.OK {
					atomic.AddInt64(&ok, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
			}
		}(time.Now().UnixNano() + int64(i))
	}
	wg.Wait()
	fmt.Printf(
		"load done: clients=%d requests=%d ok=%d failed=%d elapsed=%s\n",
		clients, requests, ok, failed, time.Since(start),
	)
	return nil
}
