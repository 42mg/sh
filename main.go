package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/42mg/decimal"
	"github.com/tidwall/buntdb"
)

type Expense struct {
	Date      string                                 `json:"date"`
	Narration string                                 `json:"narration"`
	Amount    *decimal.Decimal                       `json:"amount"`
	Breakdown map[string]*decimal.Decimal            `json:"breakdown"`
	Debt      map[string]map[string]*decimal.Decimal `json:"debt"`
}

var zero = decimal.Zero

func marshal(x any) string {
	y, _ := json.Marshal(x)
	return string(y)
}

func prettyPrint(m map[string]*decimal.Decimal) {
	var z []string
	for k, v := range m {
		z = append(z, strings.ToUpper(k)+"\t"+decimal.Text(v.Quantize(-2)))
	}

	sort.Slice(z, func(i, j int) bool {
		return z[i] < z[j]
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	for _, v := range z {
		fmt.Fprintln(w, v)
	}
	w.Flush()
}

func minMax(m map[string]*decimal.Decimal) (string, string) {
	var a, z string
	min, max := zero, zero
	for k, v := range m {
		if v.LessThan(min) {
			min = v
			a = k
		}
		if v.GreaterThan(max) {
			max = v
			z = k
		}
	}

	return a, z
}

func main() {
	rkv, err := os.ReadFile("rx.tsv")
	if err != nil {
		log.Fatalln(err)
	}

	db, err := buntdb.Open("rx.db")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	tsv := csv.NewReader(strings.NewReader(string(rkv)))
	tsv.Comma = '\t'
	tsv.FieldsPerRecord = -1

	r := make(map[string]*decimal.Decimal)
	x := zero

	for {
		t, err := tsv.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln(err)
		}

		k := strings.ToUpper(t[0])
		if len(t) != 2 {
			log.Fatalln(k + ": ratio not found.")
		}

		v, err := decimal.NewFromString(t[1])
		if err != nil {
			log.Fatalln(err)
		}

		r[k] = v
		x = x.Add(v)
	}

	minusOne := decimal.New(-1, 0)
	one := decimal.New(1, 0)

	if !x.Equal(one) {
		log.Fatalln("err: sum of ratios != 1.")
	}

	totalBreakdown := make(map[string]*decimal.Decimal)
	totalDebt := make(map[string]*decimal.Decimal)
	var tB, tD string

	var n int
	db.View(func(tx *buntdb.Tx) error {
		n, _ = tx.Len()
		return nil
	})

	if n == 0 {
		for k := range r {
			totalBreakdown[k] = zero
			totalDebt[k] = zero
		}

		tB, tD = marshal(totalBreakdown), marshal(totalDebt)

		db.Update(func(tx *buntdb.Tx) error {
			tx.Set("totalBreakdown", tB, nil)
			tx.Set("totalDebt", tD, nil)
			return nil
		})
	}

	args := os.Args[1:]
	usage := "usage: <date> <narration> <amount> <user>:<amount>..."

	if len(args) == 0 {
		fmt.Println(usage)
		return
	} else {
		if len(args) == 1 {
			cmd := strings.ToLower(args[0])

			switch cmd {
			case "-h", "--help":
				fmt.Println(usage)
				return
			case "read":
				db.View(func(tx *buntdb.Tx) error {
					tB, _ = tx.Get("totalBreakdown")
					return nil
				})

				json.Unmarshal([]byte(tB), &totalBreakdown)

				prettyPrint(totalBreakdown)

				return
			case "wq":
				db.View(func(tx *buntdb.Tx) error {
					tD, _ = tx.Get("totalDebt")
					return nil
				})

				json.Unmarshal([]byte(tD), &totalDebt)

				prettyPrint(totalDebt)

				var nX, nY int
				for _, v := range totalDebt {
					if v.LessThan(zero) {
						nX++
					} else if v.GreaterThan(zero) {
						nY++
					}
				}

				var s []string

				for {
					if nX+nY == 0 {
						return
					}

					min, max := minMax(totalDebt)

					if nX == 1 || nY == 1 {
						if nX == 1 {
							for k, v := range totalDebt {
								if v.GreaterThan(zero) {
									z := fmt.Sprintf("%v\t%v\t%v\n", min, decimal.Text(v.Quantize(-2)), k)
									s = append(s, z)
								}
							}
						} else {
							for k, v := range totalDebt {
								if v.LessThan(zero) {
									z := fmt.Sprintf("%v\t%v\t%v\n", k, decimal.Text(v.Mul(minusOne).Quantize(-2)), max)
									s = append(s, z)
								}
							}
						}

						break
					}

					if totalDebt[min].Mul(minusOne).GreaterThan(totalDebt[max]) {
						v := fmt.Sprintf("%v\t%v\t%v\n", min, decimal.Text(totalDebt[max].Quantize(-2)), max)
						s = append(s, v)
						totalDebt[min] = totalDebt[min].Add(totalDebt[max])
						delete(totalDebt, max)
						nY--
					} else if totalDebt[min].Mul(minusOne).Equal(totalDebt[max]) {
						v := fmt.Sprintf("%v\t%v\t%v\n", min, decimal.Text(totalDebt[max].Quantize(-2)), max)
						s = append(s, v)
						delete(totalDebt, min)
						delete(totalDebt, max)
						nX--
						nY--
					} else {
						v := fmt.Sprintf("%v\t%v\t%v\n", min, decimal.Text(totalDebt[min].Mul(minusOne).Quantize(-2)), max)
						s = append(s, v)
						totalDebt[max] = totalDebt[max].Add(totalDebt[min])
						delete(totalDebt, min)
						nX--
					}
				}

				sort.Slice(s, func(i, j int) bool {
					return s[i] < s[j]
				})

				fmt.Println()

				w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
				for _, v := range s {
					fmt.Fprintf(w, strings.ToUpper(v))
				}
				w.Flush()

				return
			case "yy", "yh":
				var d []string
				db.CreateIndex("x", "x:*", buntdb.IndexString)
				db.View(func(tx *buntdb.Tx) error {
					err := tx.Ascend("x", func(key, value string) bool {
						d = append(d, value)
						return true
					})
					return err
				})

				jD := "[" + strings.Join(d, ",") + "]"

				f, err := os.Create("rx.json")
				if err != nil {
					log.Fatalln(err)
				}
				defer f.Close()

				if cmd == "yh" {
					var expenses []Expense
					json.Unmarshal([]byte(jD), &expenses)
					j, _ := json.MarshalIndent(expenses, "", "	")
					jD = string(j)
				}

				_, err = f.WriteString(jD)
				if err != nil {
					log.Fatalln(err)
				}

				return
			case "last", "undo":
				if n <= 2 {
					return
				}

				var x string
				db.View(func(tx *buntdb.Tx) error {
					x, _ = tx.Get("x:" + strconv.Itoa(n-2))
					tB, _ = tx.Get("totalBreakdown")
					tD, _ = tx.Get("totalDebt")
					return nil
				})

				var expense Expense
				json.Unmarshal([]byte(x), &expense)
				json.Unmarshal([]byte(tB), &totalBreakdown)
				json.Unmarshal([]byte(tD), &totalDebt)

				date := strings.ToUpper(expense.Date)
				narration := strings.ToUpper(expense.Narration)
				amount := expense.Amount
				breakdown := strings.ToUpper(fmt.Sprint(expense.Breakdown))

				if cmd == "undo" {
					for k, v := range expense.Breakdown {
						totalBreakdown[k] = totalBreakdown[k].Sub(v)
					}

					for x, y := range expense.Debt {
						n := zero
						for k, v := range y {
							totalDebt[k] = totalDebt[k].Add(v)
							n = n.Add(v)
						}
						totalDebt[x] = totalDebt[x].Sub(n)
					}

					tB, tD = marshal(totalBreakdown), marshal(totalDebt)

					db.Update(func(tx *buntdb.Tx) error {
						tx.Delete("x:" + strconv.Itoa(n-2))
						tx.Set("totalBreakdown", tB, nil)
						tx.Set("totalDebt", tD, nil)
						return nil
					})
				}

				fmt.Println(date, narration, amount, breakdown[4:len(breakdown)-1])

				return
			default:
				log.Fatalln(cmd + ": invalid command.")
			}
		} else if len(args) < 4 {
			log.Fatalln(usage)
		}
	}

	date := args[0]
	narration := args[1]

	amount, err := decimal.NewFromString(args[2])
	if err != nil {
		log.Fatalln(err)
	}

	breakdown := make(map[string]*decimal.Decimal)
	debt := make(map[string]map[string]*decimal.Decimal)

	db.View(func(tx *buntdb.Tx) error {
		tB, _ = tx.Get("totalBreakdown")
		tD, _ = tx.Get("totalDebt")
		return nil
	})

	json.Unmarshal([]byte(tB), &totalBreakdown)
	json.Unmarshal([]byte(tD), &totalDebt)

	if strings.ToLower(args[1]) == "intra" {
		a := strings.ToUpper(args[4])
		b := strings.ToUpper(args[3])

		_, x := r[a]
		_, y := r[b]
		if !x {
			log.Fatalln(a + ": user not found.")
		} else if !y {
			log.Fatalln(b + ": user not found.")
		}

		breakdown[a] = amount.Mul(minusOne)
		breakdown[b] = amount
		m := make(map[string]*decimal.Decimal)
		m[a] = amount
		debt[b] = m
		amount = zero

		totalBreakdown[a] = totalBreakdown[a].Add(breakdown[a])
		totalBreakdown[b] = totalBreakdown[b].Add(breakdown[b])
		totalDebt[a] = totalDebt[a].Add(breakdown[a])
		totalDebt[b] = totalDebt[b].Add(breakdown[b])
	} else {
		b := zero
		for _, v := range args[3:] {
			kv := strings.Split(v, ":")
			k := strings.ToUpper(kv[0])

			_, x := r[k]
			if !x {
				log.Fatalln(k + ": user not found.")
			}

			breakdown[k] = zero

			if len(args[3:]) == 1 && len(kv) == 1 {
				breakdown[k] = amount
				b = amount
			} else {
				if len(kv) != 2 {
					log.Fatalln(usage)
				}

				f, err := decimal.NewFromString(kv[1])
				if err != nil {
					log.Fatalln(err)
				}

				breakdown[k] = breakdown[k].Add(f)
				b = b.Add(f)
			}

			totalBreakdown[k] = totalBreakdown[k].Add(breakdown[k])
		}

		if !amount.Equal(b) {
			log.Fatalln("err: amount != breakdown.")
		}

		n := zero
		rx := make(map[string]*decimal.Decimal)
		for k, v := range r {
			x := zero
			if breakdown[k] != nil {
				x = breakdown[k]
			}
			if x.LessThan(amount.Mul(v)) {
				rx[k] = amount.Mul(v).Sub(x).Div(amount)
				n = n.Add(rx[k])
			}
		}

		for k, v := range breakdown {
			diff := v.Sub(amount.Mul(r[k]))
			m := make(map[string]*decimal.Decimal)
			if diff.GreaterThan(zero) {
				for k, v := range rx {
					m[k] = diff.Mul(v).Div(n)
					totalDebt[k] = totalDebt[k].Sub(m[k])
				}
				debt[k] = m
				totalDebt[k] = totalDebt[k].Add(diff)
			}
		}
	}

	expense := Expense{
		Date:      date,
		Narration: narration,
		Amount:    amount,
		Breakdown: breakdown,
		Debt:      debt,
	}

	jD, tB, tD := marshal(expense), marshal(totalBreakdown), marshal(totalDebt)

	db.Update(func(tx *buntdb.Tx) error {
		n, _ := tx.Len()
		tx.Set("x:"+strconv.Itoa(n-1), jD, nil)
		tx.Set("totalBreakdown", tB, nil)
		tx.Set("totalDebt", tD, nil)
		return nil
	})
}
